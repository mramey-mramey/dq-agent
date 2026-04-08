"""
backend/agent.py

Claude API orchestration layer for the DQ agent.

This module is the single integration point between the Claude API and all
backend tool functions. It owns:

  1. Session state  — DatasetMeta, working DataFrames, Issue registry,
                      AuditLog, and DB connection strings are all held in
                      AgentSession objects keyed by session_id.

  2. Tool registry  — maps every tool name from CLAUDE.md to the concrete
                      Python function that implements it, plus JSON schemas
                      for the Claude API.

  3. Agentic loop   — run_agent_turn() sends a user message + conversation
                      history to the Claude API, handles tool_use blocks by
                      dispatching to registered handlers, feeds tool_result
                      blocks back, and iterates until Claude returns a final
                      text response (stop_reason == "end_turn").

  4. Tool handlers  — thin adapter functions that translate Claude's
                      tool_input dicts into typed Python calls, then
                      serialise the result back to a JSON-safe string
                      for the API response.

Design constraints (from CLAUDE.md):
  - execute_approved_cleanse is gated server-side in cleanse.py; the handler
    here additionally validates that approved_by is present before calling.
  - DB connection strings are held in AgentSession.connection_strings, never
    logged, never written to DatasetMeta.
  - All tool results returned to Claude are plain JSON strings; no DataFrames
    or Pydantic models are sent over the wire.
  - Conversation history is the caller's responsibility — pass the full
    history on every turn (stateless from Claude's perspective).
  - Max tool-call iterations per turn is bounded by MAX_TOOL_ITERATIONS to
    prevent runaway loops.

Usage (from FastAPI route or tests):
    session = AgentSession()
    history = []
    response = await run_agent_turn(
        session=session,
        user_message="Ingest /uploads/vendors.csv and run all quality checks.",
        history=history,
    )
    history.extend(response.new_history)
    print(response.text)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import anthropic
import pandas as pd

from backend.models.dataset import DatasetMeta
from backend.models.issue import ActionType, Issue, IssueStatus, ProposedAction
from backend.tools.cleanse import AuditLog, execute_all_approved, execute_approved_cleanse, record_rejection
from backend.tools.entity_resolution import SignalConfig, auto_detect_config, resolve_entities
from backend.tools.export import export_clean_file, export_clean_table
from backend.tools.ingest import ingest_db_table, ingest_file
from backend.tools.quality_checks import DomainRule, run_quality_checks

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 4096
MAX_TOOL_ITERATIONS = 10   # Hard ceiling on tool calls per user turn

SYSTEM_PROMPT = """You are a Data Quality Agent for an FP&A organization. Your job is to analyze \
incoming datasets, identify quality issues, and propose precise, minimal cleansing actions to correct them.

You have access to tools that allow you to ingest data, run quality checks, retrieve issue details, \
stage cleansing proposals, execute approved changes, and export clean output.

IMPORTANT RULES:
- You MUST NOT call execute_approved_cleanse unless the issue status is APPROVED and an approved_by \
value is present. Never modify data speculatively.
- When evaluating entity matching issues (e.g., duplicate vendor names), reason through string \
similarity, context clues (address, tax ID, contact), and historical transaction patterns before \
proposing a canonical form.
- Always return findings in structured JSON when calling tools. Be conservative: when confidence \
is below 0.80, flag for human review rather than proposing an automatic fix.
- Explain your reasoning for each issue in plain language suitable for a non-technical business analyst.
- After ingesting data and running quality checks, summarize the findings clearly before asking \
what the user would like to do next.
- Never invent issue_ids, dataset_ids, or other identifiers — only use values returned by tools."""


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------


@dataclass
class AgentSession:
    """
    All mutable state for a single dataset run.

    One session per dataset run. The FastAPI layer creates a session on
    ingest and stores it server-side keyed by session_id. It is passed
    into every run_agent_turn() call.
    """

    session_id: str = field(
        default_factory=lambda: f"sess-{__import__('uuid').uuid4().hex[:12]}"
    )
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # Populated by ingest tool handlers
    dataset_meta: DatasetMeta | None = None
    dataframe: pd.DataFrame | None = None      # Working copy; updated by cleanse

    # Issue registry — keyed by issue_id
    issues: dict[str, Issue] = field(default_factory=dict)

    # Append-only audit log for this session
    audit_log: AuditLog = field(default_factory=AuditLog)

    # DB connection string (Mode B) — held here, never written to meta or logs
    connection_string: str | None = None

    def get_issue(self, issue_id: str) -> Issue | None:
        return self.issues.get(issue_id)

    def register_issues(self, new_issues: list[Issue]) -> None:
        for issue in new_issues:
            self.issues[issue.issue_id] = issue

    def approved_issues(self) -> list[Issue]:
        return [i for i in self.issues.values() if i.status == IssueStatus.APPROVED]

    def pending_issues(self) -> list[Issue]:
        return [i for i in self.issues.values() if i.status == IssueStatus.PENDING_APPROVAL]


# ---------------------------------------------------------------------------
# Agent turn result
# ---------------------------------------------------------------------------


@dataclass
class AgentTurnResult:
    """Returned by run_agent_turn() to the caller (FastAPI route or test)."""

    text: str                              # Claude's final text response
    new_history: list[dict]               # Messages to append to conversation history
    tool_calls_made: list[str] = field(default_factory=list)  # Tool names invoked
    error: str | None = None              # Set if the turn failed entirely


# ---------------------------------------------------------------------------
# Tool definitions (Claude API schema)
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "ingest_file",
        "description": (
            "Read a CSV or Excel file into the agent's working dataset. "
            "Returns dataset_id, row count, column names, and inferred types. "
            "Call this first before running any quality checks."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Absolute path to the uploaded file."
                },
                "sheet_name": {
                    "type": "string",
                    "description": "Excel only. Defaults to first sheet if omitted."
                },
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "ingest_db_table",
        "description": (
            "Read a database table or query result into the agent's working dataset "
            "via a SQLAlchemy connection. Connection is opened read-only. "
            "Call this first before running any quality checks."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "connection_string": {
                    "type": "string",
                    "description": "SQLAlchemy URL, e.g. postgresql+psycopg2://user:pass@host/db"
                },
                "table_or_query": {
                    "type": "string",
                    "description": "Table name (e.g. 'vendors') or a full SELECT query."
                },
            },
            "required": ["connection_string", "table_or_query"],
        },
    },
    {
        "name": "run_quality_checks",
        "description": (
            "Run all configured DQ rules against the ingested dataset. "
            "Returns a structured report with all issues found, their severity, "
            "confidence scores, and proposed fixes. Run after ingest_file or ingest_db_table."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {
                    "type": "string",
                    "description": "The dataset_id returned by the ingest tool."
                },
                "rule_categories": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Subset of categories to run: COMPLETENESS, UNIQUENESS, "
                        "DEDUPLICATION, FORMAT_VALIDITY, CONSISTENCY, OUTLIER, DOMAIN_RULE. "
                        "Omit to run all."
                    ),
                },
            },
            "required": ["dataset_id"],
        },
    },
    {
        "name": "run_entity_resolution",
        "description": (
            "Run multi-signal entity resolution across the dataset to find records "
            "that represent the same real-world entity across multiple columns "
            "(name, address, tax ID, etc.). More powerful than single-column deduplication. "
            "Returns a list of entity clusters with confidence scores and canonical records."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {
                    "type": "string",
                    "description": "The dataset_id returned by the ingest tool."
                },
                "signal_columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Column names to use as matching signals. "
                        "If omitted, defaults to VENDOR_SIGNALS preset "
                        "(vendor_name, tax_id, address, zip, city, state, payment_terms)."
                    ),
                },
                "id_column": {
                    "type": "string",
                    "description": "Optional column to use as record identifier in explanations."
                },
            },
            "required": ["dataset_id"],
        },
    },
    {
        "name": "get_issue_details",
        "description": (
            "Retrieve full details for a specific DQ issue including its current status, "
            "affected records, raw values, and proposed fix."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "issue_id": {
                    "type": "string",
                    "description": "The issue_id from a quality check or entity resolution report."
                },
            },
            "required": ["issue_id"],
        },
    },
    {
        "name": "list_issues",
        "description": (
            "List all issues for the current dataset, optionally filtered by status or category. "
            "Returns issue_id, category, severity, status, and description for each."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "status_filter": {
                    "type": "string",
                    "description": "Filter by status: OPEN, PENDING_APPROVAL, APPROVED, REJECTED, EXECUTED.",
                },
                "category_filter": {
                    "type": "string",
                    "description": "Filter by category, e.g. DEDUPLICATION or COMPLETENESS.",
                },
            },
            "required": ["dataset_id"],
        },
    },
    {
        "name": "execute_approved_cleanse",
        "description": (
            "Execute a cleansing action that has been explicitly approved by an authorized user. "
            "The issue MUST be in APPROVED status — this is enforced server-side. "
            "Writes an immutable audit log entry. "
            "IMPORTANT: Only call this when approved_by is confirmed present."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "issue_id": {
                    "type": "string",
                    "description": "The issue_id to execute."
                },
                "approved_by": {
                    "type": "string",
                    "description": "Username of the human who approved this action."
                },
            },
            "required": ["issue_id", "approved_by"],
        },
    },
    {
        "name": "execute_all_approved",
        "description": (
            "Execute all issues currently in APPROVED status for this dataset in the correct order. "
            "Skips FLAG_ONLY and non-approved issues. Returns a summary of what was executed. "
            "Use this after a batch approval to apply all fixes at once."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "approved_by": {
                    "type": "string",
                    "description": "Username of the human who approved the batch."
                },
            },
            "required": ["dataset_id", "approved_by"],
        },
    },
    {
        "name": "export_clean_file",
        "description": (
            "Write the clean dataset to a new output file (CSV or Excel) after all approved "
            "cleanses have been executed. Returns the output file path for download. "
            "Only valid for file upload (Mode A) sessions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "output_filename": {
                    "type": "string",
                    "description": "Optional filename override. Defaults to {original}_clean_{timestamp}."
                },
            },
            "required": ["dataset_id"],
        },
    },
    {
        "name": "export_clean_table",
        "description": (
            "Write the clean dataset to a new table in the source database after all approved "
            "cleanses have been executed. Returns the output table name. "
            "Only valid for live DB connection (Mode B) sessions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "output_table_name": {
                    "type": "string",
                    "description": "Optional table name override. Defaults to {source_table}_clean_{timestamp}."
                },
                "if_exists": {
                    "type": "string",
                    "enum": ["fail", "replace"],
                    "description": "Behaviour if table already exists. Defaults to 'fail'."
                },
            },
            "required": ["dataset_id"],
        },
    },
    {
        "name": "generate_dq_scorecard",
        "description": (
            "Generate a DQ scorecard summarizing issue counts by category and severity, "
            "pass/fail rates, and overall data quality score for the dataset."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
            },
            "required": ["dataset_id"],
        },
    },
]


# ---------------------------------------------------------------------------
# Agentic loop
# ---------------------------------------------------------------------------


async def run_agent_turn(
    session: AgentSession,
    user_message: str,
    history: list[dict],
    *,
    approved_by: str | None = None,
) -> AgentTurnResult:
    """
    Run one full agent turn: send the user message, handle all tool calls,
    and return Claude's final text response.

    Args:
        session:      The AgentSession for this dataset run.
        user_message: The user's latest message.
        history:      Full conversation history (list of message dicts).
                      Should include all prior turns. This is stateless from
                      Claude's perspective — the full history is sent each time.
        approved_by:  If set, passed to execution tools requiring an approver.

    Returns:
        AgentTurnResult with text, new_history entries to append, and
        the list of tool names that were called.
    """
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    # Build message list: history + new user message
    messages: list[dict] = list(history) + [
        {"role": "user", "content": user_message}
    ]

    new_history: list[dict] = [{"role": "user", "content": user_message}]
    tool_calls_made: list[str] = []

    for iteration in range(MAX_TOOL_ITERATIONS):
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            messages=list(messages),  # Pass a snapshot; mutations after the call don't affect call_args
        )

        # Collect all content blocks from this response
        assistant_content = response.content
        messages.append({"role": "assistant", "content": assistant_content})
        new_history.append({"role": "assistant", "content": assistant_content})

        # If Claude is done with tool calls, return its text
        if response.stop_reason == "end_turn":
            final_text = _extract_text(assistant_content)
            return AgentTurnResult(
                text=final_text,
                new_history=new_history,
                tool_calls_made=tool_calls_made,
            )

        # If Claude wants to use tools, dispatch each one
        if response.stop_reason == "tool_use":
            tool_results: list[dict] = []

            for block in assistant_content:
                if block.type != "tool_use":
                    continue

                tool_name = block.name
                tool_input = block.input
                tool_use_id = block.id
                tool_calls_made.append(tool_name)

                logger.info(
                    "session=%s | tool=%s | input=%s",
                    session.session_id,
                    tool_name,
                    json.dumps(tool_input, default=str)[:200],
                )

                result_content = _dispatch_tool(
                    tool_name=tool_name,
                    tool_input=tool_input,
                    session=session,
                    approved_by=approved_by,
                )

                logger.info(
                    "session=%s | tool=%s | result=%s",
                    session.session_id,
                    tool_name,
                    result_content[:200],
                )

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": result_content,
                })

            # Feed tool results back for the next iteration
            messages.append({"role": "user", "content": tool_results})
            new_history.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason
        break

    # Exhausted iterations without end_turn
    final_text = _extract_text(assistant_content) or (
        f"Agent reached the maximum tool call limit ({MAX_TOOL_ITERATIONS}) "
        "without completing. Please try a more focused request."
    )
    return AgentTurnResult(
        text=final_text,
        new_history=new_history,
        tool_calls_made=tool_calls_made,
        error="max_iterations_reached",
    )


# ---------------------------------------------------------------------------
# Tool dispatcher
# ---------------------------------------------------------------------------


def _dispatch_tool(
    tool_name: str,
    tool_input: dict,
    session: AgentSession,
    approved_by: str | None,
) -> str:
    """
    Route a tool call to its handler and return a JSON string result.
    All handlers catch exceptions and return structured error dicts rather
    than crashing the agentic loop.
    """
    handlers = {
        "ingest_file":             _handle_ingest_file,
        "ingest_db_table":         _handle_ingest_db_table,
        "run_quality_checks":      _handle_run_quality_checks,
        "run_entity_resolution":   _handle_run_entity_resolution,
        "get_issue_details":       _handle_get_issue_details,
        "list_issues":             _handle_list_issues,
        "execute_approved_cleanse":_handle_execute_approved_cleanse,
        "execute_all_approved":    _handle_execute_all_approved,
        "export_clean_file":       _handle_export_clean_file,
        "export_clean_table":      _handle_export_clean_table,
        "generate_dq_scorecard":   _handle_generate_dq_scorecard,
    }

    handler = handlers.get(tool_name)
    if handler is None:
        return _err(f"Unknown tool: '{tool_name}'")

    try:
        return handler(tool_input, session, approved_by)
    except Exception as exc:
        logger.exception("Unhandled error in tool handler '%s': %s", tool_name, exc)
        return _err(f"Internal error in {tool_name}: {exc}")


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------


def _handle_ingest_file(
    inp: dict, session: AgentSession, _approved_by: str | None
) -> str:
    result = ingest_file(
        file_path=inp["file_path"],
        sheet_name=inp.get("sheet_name"),
    )
    if not result.success:
        return _err(result.error)

    session.dataset_meta = result.dataset_meta
    session.dataframe = result.dataframe

    meta = result.dataset_meta
    return _ok({
        "dataset_id": meta.dataset_id,
        "source_type": meta.source_type.value,
        "row_count": meta.row_count,
        "column_count": meta.column_count,
        "columns": [
            {
                "name": c.name,
                "dtype": c.dtype,
                "nullable": c.nullable,
                "unique_count": c.unique_count,
            }
            for c in meta.columns
        ],
        "output_filename": meta.output_filename,
        "summary": result.summary,
    })


def _handle_ingest_db_table(
    inp: dict, session: AgentSession, _approved_by: str | None
) -> str:
    conn_str = inp["connection_string"]
    result = ingest_db_table(
        connection_string=conn_str,
        table_or_query=inp["table_or_query"],
    )
    if not result.success:
        return _err(result.error)

    session.dataset_meta = result.dataset_meta
    session.dataframe = result.dataframe
    session.connection_string = conn_str   # Held in session, never in meta

    meta = result.dataset_meta
    return _ok({
        "dataset_id": meta.dataset_id,
        "source_type": meta.source_type.value,
        "row_count": meta.row_count,
        "column_count": meta.column_count,
        "columns": [
            {
                "name": c.name,
                "dtype": c.dtype,
                "nullable": c.nullable,
                "unique_count": c.unique_count,
            }
            for c in meta.columns
        ],
        "output_table": meta.output_table,
        "summary": result.summary,
    })


def _handle_run_quality_checks(
    inp: dict, session: AgentSession, _approved_by: str | None
) -> str:
    dataset_id = inp["dataset_id"]
    err = _check_session(session, dataset_id)
    if err:
        return err

    report = run_quality_checks(
        df=session.dataframe,
        meta=session.dataset_meta,
        rule_categories=inp.get("rule_categories"),
    )
    session.register_issues(report.issues)

    return _ok({
        "dataset_id": dataset_id,
        "run_id": report.run_id,
        "total_issues": report.total_count,
        "critical": report.critical_count,
        "high": report.high_count,
        "medium": report.medium_count,
        "low": report.low_count,
        "categories_checked": report.categories_checked,
        "issues": [
            {
                "issue_id": i.issue_id,
                "category": i.category.value,
                "severity": i.severity.value,
                "status": i.status.value,
                "confidence": i.confidence,
                "description": i.description,
                "affected_columns": i.affected_columns,
                "affected_row_count": len(i.affected_row_indices),
                "is_actionable": i.is_actionable(),
                "can_bulk_approve": i.can_bulk_approve(
                    float(os.getenv("DQ_BULK_APPROVE_THRESHOLD", "0.95"))
                ),
            }
            for i in report.issues
        ],
        "summary": report.summary_text(),
    })


def _handle_run_entity_resolution(
    inp: dict, session: AgentSession, _approved_by: str | None
) -> str:
    dataset_id = inp["dataset_id"]
    err = _check_session(session, dataset_id)
    if err:
        return err

    # Build signal config — use auto-detection or caller-supplied columns
    signal_columns: list[str] | None = inp.get("signal_columns")
    if signal_columns:
        config = SignalConfig(name_columns=signal_columns)
    else:
        config = auto_detect_config(session.dataframe)

    er_result = resolve_entities(
        df=session.dataframe,
        meta=session.dataset_meta,
        config=config,
    )

    # Register generated issues in session
    session.register_issues(er_result.issues)

    return _ok({
        "dataset_id": dataset_id,
        "clusters_found": len(er_result.clusters),
        "issues_registered": len(er_result.issues),
        "pairs_evaluated": er_result.pairs_evaluated,
        "summary": er_result.summary,
        "clusters": [
            {
                "cluster_id": c.cluster_id,
                "confidence": c.confidence,
                "row_count": len(c.row_indices),
                "retain_row": c.retain_index,
                "retire_rows": c.retire_indices,
                "explanation": c.evidence_summary,
                "canonical_record": {
                    k: v for k, v in c.canonical_record.items()
                    if v is not None
                },
            }
            for c in er_result.clusters
        ],
    })


def _handle_get_issue_details(
    inp: dict, session: AgentSession, _approved_by: str | None
) -> str:
    issue_id = inp["issue_id"]
    issue = session.get_issue(issue_id)
    if issue is None:
        return _err(f"Issue '{issue_id}' not found in this session.")

    pa = issue.proposed_action
    return _ok({
        "issue_id": issue.issue_id,
        "dataset_id": issue.dataset_id,
        "category": issue.category.value,
        "severity": issue.severity.value,
        "issue_status": issue.status.value,
        "confidence": issue.confidence,
        "description": issue.description,
        "affected_columns": issue.affected_columns,
        "affected_row_indices": issue.affected_row_indices,
        "raw_values": issue.raw_values,
        "is_actionable": issue.is_actionable(),
        "proposed_action": {
            "action_type": pa.action_type.value,
            "target_column": pa.target_column,
            "target_row_indices": pa.target_row_indices,
            "canonical_value": pa.canonical_value
                if not isinstance(pa.canonical_value, dict)
                else "<canonical record — see entity resolution output>",
            "retain_row_index": pa.retain_row_index,
            "retire_row_indices": pa.retire_row_indices,
            "rationale": pa.rationale,
        } if pa else None,
        "detected_at": issue.detected_at.isoformat(),
        "approved_by": issue.approved_by,
        "reviewer_note": issue.reviewer_note,
    })


def _handle_list_issues(
    inp: dict, session: AgentSession, _approved_by: str | None
) -> str:
    dataset_id = inp["dataset_id"]
    err = _check_session(session, dataset_id)
    if err:
        return err

    status_filter = inp.get("status_filter", "").upper() or None
    category_filter = inp.get("category_filter", "").upper() or None

    issues = list(session.issues.values())
    if status_filter:
        issues = [i for i in issues if i.status.value == status_filter]
    if category_filter:
        issues = [i for i in issues if i.category.value == category_filter]

    return _ok({
        "dataset_id": dataset_id,
        "total": len(issues),
        "issues": [
            {
                "issue_id": i.issue_id,
                "category": i.category.value,
                "severity": i.severity.value,
                "status": i.status.value,
                "confidence": i.confidence,
                "description": i.description[:120] + "..."
                    if len(i.description) > 120 else i.description,
                "affected_columns": i.affected_columns,
                "is_actionable": i.is_actionable(),
            }
            for i in issues
        ],
    })


def _handle_execute_approved_cleanse(
    inp: dict, session: AgentSession, approved_by: str | None
) -> str:
    issue_id = inp["issue_id"]
    actor = inp.get("approved_by") or approved_by
    if not actor:
        return _err(
            "approved_by is required to execute a cleanse. "
            "The agent must not execute without an identified human approver."
        )

    issue = session.get_issue(issue_id)
    if issue is None:
        return _err(f"Issue '{issue_id}' not found in this session.")

    if session.dataframe is None:
        return _err("No working DataFrame in session. Ingest a dataset first.")

    result = execute_approved_cleanse(
        issue=issue,
        df=session.dataframe,
        approved_by=actor,
        audit_log=session.audit_log,
    )

    if result.success and result.clean_df is not None:
        session.dataframe = result.clean_df

    return _ok({
        "success": result.success,
        "issue_id": result.issue_id,
        "action_type": result.action_type,
        "rows_affected": result.rows_affected,
        "error": result.error,
    })


def _handle_execute_all_approved(
    inp: dict, session: AgentSession, approved_by: str | None
) -> str:
    dataset_id = inp["dataset_id"]
    err = _check_session(session, dataset_id)
    if err:
        return err

    actor = inp.get("approved_by") or approved_by
    if not actor:
        return _err("approved_by is required to execute cleanses.")

    if session.dataframe is None:
        return _err("No working DataFrame in session.")

    issues = list(session.issues.values())
    bulk = execute_all_approved(
        issues=issues,
        df=session.dataframe,
        approved_by=actor,
        audit_log=session.audit_log,
    )

    if bulk.clean_df is not None:
        session.dataframe = bulk.clean_df

    return _ok({
        "total_issues": bulk.total,
        "succeeded": bulk.succeeded,
        "failed": bulk.failed,
        "skipped": bulk.skipped,
        "all_succeeded": bulk.all_succeeded,
        "results": [
            {
                "issue_id": r.issue_id,
                "action_type": r.action_type,
                "rows_affected": r.rows_affected,
                "success": r.success,
                "error": r.error,
            }
            for r in bulk.results
        ],
    })


def _handle_export_clean_file(
    inp: dict, session: AgentSession, approved_by: str | None
) -> str:
    dataset_id = inp["dataset_id"]
    err = _check_session(session, dataset_id)
    if err:
        return err

    if session.dataframe is None:
        return _err("No working DataFrame in session.")

    actor = approved_by or "SYSTEM"
    result = export_clean_file(
        df=session.dataframe,
        meta=session.dataset_meta,
        output_filename=inp.get("output_filename"),
        exported_by=actor,
        audit_log=session.audit_log,
    )

    return _ok({
        "success": result.success,
        "output_path": result.output_path,
        "row_count": result.row_count,
        "column_count": result.column_count,
        "summary": result.summary,
        "error": result.error,
    })


def _handle_export_clean_table(
    inp: dict, session: AgentSession, approved_by: str | None
) -> str:
    dataset_id = inp["dataset_id"]
    err = _check_session(session, dataset_id)
    if err:
        return err

    if session.dataframe is None:
        return _err("No working DataFrame in session.")

    if not session.connection_string:
        return _err(
            "No database connection string in session. "
            "This dataset was not ingested via ingest_db_table."
        )

    actor = approved_by or "SYSTEM"
    result = export_clean_table(
        df=session.dataframe,
        meta=session.dataset_meta,
        connection_string=session.connection_string,
        output_table=inp.get("output_table_name"),
        if_exists=inp.get("if_exists", "fail"),
        exported_by=actor,
        audit_log=session.audit_log,
    )

    return _ok({
        "success": result.success,
        "output_table": result.output_table,
        "row_count": result.row_count,
        "column_count": result.column_count,
        "summary": result.summary,
        "error": result.error,
    })


def _handle_generate_dq_scorecard(
    inp: dict, session: AgentSession, _approved_by: str | None
) -> str:
    dataset_id = inp["dataset_id"]
    err = _check_session(session, dataset_id)
    if err:
        return err

    issues = list(session.issues.values())
    if not issues:
        return _ok({
            "dataset_id": dataset_id,
            "message": "No issues found. Run quality checks first.",
            "overall_score": 100.0,
            "total_issues": 0,
            "severity_breakdown": {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0},
            "status_breakdown": {"OPEN": 0, "PENDING_APPROVAL": 0, "APPROVED": 0, "REJECTED": 0, "EXECUTED": 0},
            "by_category": {},
            "rows_with_issues": 0,
            "total_rows": session.dataset_meta.row_count if session.dataset_meta else 0,
        })

    meta = session.dataset_meta
    total_rows = meta.row_count if meta else 1

    # Tally by category
    by_category: dict[str, dict] = {}
    for issue in issues:
        cat = issue.category.value
        if cat not in by_category:
            by_category[cat] = {"total": 0, "high_critical": 0, "affected_rows": set()}
        by_category[cat]["total"] += 1
        if issue.severity.value in ("HIGH", "CRITICAL"):
            by_category[cat]["high_critical"] += 1
        by_category[cat]["affected_rows"].update(issue.affected_row_indices)

    # Overall score: fraction of rows with no HIGH/CRITICAL issues
    all_affected = set()
    for issue in issues:
        if issue.severity.value in ("HIGH", "CRITICAL"):
            all_affected.update(issue.affected_row_indices)

    clean_rows = max(0, total_rows - len(all_affected))
    overall_score = round((clean_rows / total_rows) * 100, 1) if total_rows > 0 else 100.0

    severity_counts = {
        "CRITICAL": sum(1 for i in issues if i.severity.value == "CRITICAL"),
        "HIGH":     sum(1 for i in issues if i.severity.value == "HIGH"),
        "MEDIUM":   sum(1 for i in issues if i.severity.value == "MEDIUM"),
        "LOW":      sum(1 for i in issues if i.severity.value == "LOW"),
    }

    status_counts = {
        "OPEN":             sum(1 for i in issues if i.status.value == "OPEN"),
        "PENDING_APPROVAL": sum(1 for i in issues if i.status.value == "PENDING_APPROVAL"),
        "APPROVED":         sum(1 for i in issues if i.status.value == "APPROVED"),
        "REJECTED":         sum(1 for i in issues if i.status.value == "REJECTED"),
        "EXECUTED":         sum(1 for i in issues if i.status.value == "EXECUTED"),
    }

    return _ok({
        "dataset_id": dataset_id,
        "overall_score": overall_score,
        "total_issues": len(issues),
        "severity_breakdown": severity_counts,
        "status_breakdown": status_counts,
        "by_category": {
            cat: {
                "total_issues": v["total"],
                "high_critical_issues": v["high_critical"],
                "affected_row_count": len(v["affected_rows"]),
            }
            for cat, v in by_category.items()
        },
        "rows_with_issues": len(all_affected),
        "total_rows": total_rows,
        "note": (
            f"Overall score reflects the percentage of rows free of HIGH/CRITICAL issues. "
            f"{len(all_affected):,} of {total_rows:,} rows have at least one significant issue."
        ),
    })


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------


def _check_session(session: AgentSession, dataset_id: str) -> str | None:
    """Return an error string if session has no data or dataset_id mismatches."""
    if session.dataset_meta is None:
        return _err("No dataset has been ingested in this session. Call ingest_file or ingest_db_table first.")
    if session.dataset_meta.dataset_id != dataset_id:
        return _err(
            f"dataset_id '{dataset_id}' does not match the current session dataset "
            f"'{session.dataset_meta.dataset_id}'. Each session handles one dataset at a time."
        )
    return None


def _ok(data: dict) -> str:
    """Serialise a success result dict to JSON string for Claude."""
    return json.dumps({"status": "ok", **data}, default=str)


def _err(message: str) -> str:
    """Serialise an error result to JSON string for Claude."""
    return json.dumps({"status": "error", "error": message}, default=str)


def _extract_text(content: list) -> str:
    """Extract plain text from a list of content blocks."""
    parts = []
    for block in content:
        if hasattr(block, "type") and block.type == "text":
            parts.append(block.text)
        elif isinstance(block, dict) and block.get("type") == "text":
            parts.append(block["text"])
    return "\n".join(parts).strip()