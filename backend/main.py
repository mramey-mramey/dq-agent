"""
backend/main.py

FastAPI application for the DQ Agent.

Exposes the agent and all tool operations as a REST API consumed by the
Streamlit frontend (and optionally by external callers).

Session lifecycle:
    POST /sessions/                → Create session, returns session_id
    POST /sessions/{id}/ingest/file   → Upload CSV/Excel, triggers ingest
    POST /sessions/{id}/ingest/db     → Connect to live DB, triggers ingest
    POST /sessions/{id}/chat          → Send a message to the agent
    GET  /sessions/{id}/issues        → List issues (with optional filters)
    GET  /sessions/{id}/issues/{iid}  → Get full issue detail
    POST /sessions/{id}/issues/{iid}/approve  → Approve a single issue
    POST /sessions/{id}/issues/{iid}/reject   → Reject a single issue
    POST /sessions/{id}/approve-all           → Bulk approve eligible issues
    POST /sessions/{id}/execute               → Execute all approved issues
    POST /sessions/{id}/export/file           → Export clean file (Mode A)
    POST /sessions/{id}/export/table          → Export clean table (Mode B)
    GET  /sessions/{id}/scorecard             → DQ scorecard
    GET  /sessions/{id}/audit                 → Audit log entries
    GET  /health                              → Health check

RBAC:
    Role is passed in the X-User-Role header (Viewer/Analyst/Senior Analyst/Admin).
    Approval and execution endpoints enforce minimum role requirements.
    In production, replace the header-based role extraction with a real
    auth provider (JWT, OAuth2, etc.).

Design notes:
    - Sessions are stored in an in-process dict. In production, move to Redis
      or a DB-backed store for durability and horizontal scaling.
    - Uploaded files are written to a secure temp directory, not the cwd.
    - DB connection strings from ingest/db are held only in AgentSession;
      they are never echoed back in any response.
    - All approval/execution routes require approved_by from the request body;
      they do not infer the approver from the session.
"""

from __future__ import annotations

import logging
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    Header,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from backend.agent import AgentSession, AgentTurnResult, run_agent_turn
from backend.models.issue import IssueStatus
from backend.tools.cleanse import execute_all_approved, record_rejection
from backend.tools.export import export_clean_file, export_clean_table
from backend.tools.ingest import ingest_db_table, ingest_file

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="DQ Agent API",
    description="AI-powered data quality and cleansing agent for FP&A pipelines.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-process session store — replace with Redis/DB for production
_sessions: dict[str, AgentSession] = {}

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", tempfile.mkdtemp(prefix="dq_uploads_")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/tmp/dq_outputs"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# RBAC helpers
# ---------------------------------------------------------------------------

ROLE_HIERARCHY = {
    "viewer": 0,
    "analyst": 1,
    "senior analyst": 2,
    "admin": 3,
}


def _require_role(minimum: str, x_user_role: str = Header(default="analyst")) -> str:
    """Dependency: enforce minimum role level via X-User-Role header."""
    role = x_user_role.lower().strip()
    if ROLE_HIERARCHY.get(role, -1) < ROLE_HIERARCHY.get(minimum, 99):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Role '{x_user_role}' does not have permission for this action. "
                   f"Minimum required: '{minimum}'.",
        )
    return role


def _get_session(session_id: str) -> AgentSession:
    """Dependency: look up session or 404."""
    session = _sessions.get(session_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session '{session_id}' not found.",
        )
    return session


def _get_issue(session: AgentSession, issue_id: str):
    """Look up issue in session or 404."""
    issue = session.get_issue(issue_id)
    if issue is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Issue '{issue_id}' not found in session.",
        )
    return issue


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class SessionResponse(BaseModel):
    session_id: str
    created_at: datetime


class ChatRequest(BaseModel):
    message: str
    history: list[dict] = Field(default_factory=list)


class ChatResponse(BaseModel):
    text: str
    tool_calls_made: list[str]
    new_history: list[dict]


class IngestDBRequest(BaseModel):
    connection_string: str
    table_or_query: str


class ApproveRequest(BaseModel):
    approved_by: str
    note: str = ""


class RejectRequest(BaseModel):
    rejected_by: str
    note: str = ""


class BulkApproveRequest(BaseModel):
    approved_by: str
    confidence_threshold: float = float(os.getenv("DQ_BULK_APPROVE_THRESHOLD", "0.95"))


class ExportFileRequest(BaseModel):
    output_filename: str | None = None
    exported_by: str = "user"


class ExportTableRequest(BaseModel):
    output_table_name: str | None = None
    if_exists: str = "fail"
    exported_by: str = "user"


class IssueListItem(BaseModel):
    issue_id: str
    category: str
    severity: str
    status: str
    confidence: float
    description: str
    affected_columns: list[str]
    is_actionable: bool
    can_bulk_approve: bool


class AuditEntryResponse(BaseModel):
    entry_id: str
    timestamp: datetime
    issue_id: str
    action_type: str
    actor: str
    success: bool
    notes: str
    affected_row_count: int


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
def health_check():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

@app.post("/sessions/", response_model=SessionResponse, tags=["Sessions"])
def create_session():
    """Create a new agent session. Returns a session_id for all subsequent calls."""
    session = AgentSession()
    _sessions[session.session_id] = session
    logger.info("Created session %s", session.session_id)
    return SessionResponse(
        session_id=session.session_id,
        created_at=session.created_at,
    )


@app.delete("/sessions/{session_id}", tags=["Sessions"])
def delete_session(
    session_id: str,
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("analyst", x_user_role)),
):
    """Delete a session and free its resources."""
    if session_id not in _sessions:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found.")
    del _sessions[session_id]
    return {"deleted": session_id}


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

@app.post("/sessions/{session_id}/ingest/file", tags=["Ingest"])
async def ingest_file_endpoint(
    session_id: str,
    file: UploadFile = File(...),
    sheet_name: str | None = Form(default=None),
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("analyst", x_user_role)),
):
    """
    Upload a CSV or Excel file and ingest it into the session.
    Returns dataset_id, row count, column metadata, and derived output filename.
    """
    # Write upload to secure temp path
    suffix = Path(file.filename or "upload").suffix.lower() or ".csv"
    tmp_path = UPLOAD_DIR / f"{uuid.uuid4().hex}{suffix}"
    try:
        content = await file.read()
        tmp_path.write_bytes(content)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {exc}")

    result = ingest_file(str(tmp_path), sheet_name=sheet_name)
    if not result.success:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=result.error)

    session.dataset_meta = result.dataset_meta
    session.dataframe = result.dataframe
    meta = result.dataset_meta

    return {
        "dataset_id": meta.dataset_id,
        "source_type": meta.source_type.value,
        "original_filename": meta.original_filename,
        "row_count": meta.row_count,
        "column_count": meta.column_count,
        "columns": [
            {"name": c.name, "dtype": c.dtype, "nullable": c.nullable}
            for c in meta.columns
        ],
        "output_filename": meta.output_filename,
        "summary": result.summary,
    }


@app.post("/sessions/{session_id}/ingest/db", tags=["Ingest"])
def ingest_db_endpoint(
    session_id: str,
    body: IngestDBRequest,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("analyst", x_user_role)),
):
    """
    Connect to a live database and ingest a table or query result.
    The connection string is held in session memory only — never persisted.
    """
    result = ingest_db_table(body.connection_string, body.table_or_query)
    if not result.success:
        raise HTTPException(status_code=422, detail=result.error)

    session.dataset_meta = result.dataset_meta
    session.dataframe = result.dataframe
    session.connection_string = body.connection_string
    meta = result.dataset_meta

    return {
        "dataset_id": meta.dataset_id,
        "source_type": meta.source_type.value,
        "row_count": meta.row_count,
        "column_count": meta.column_count,
        "columns": [
            {"name": c.name, "dtype": c.dtype, "nullable": c.nullable}
            for c in meta.columns
        ],
        "output_table": meta.output_table,
        "summary": result.summary,
    }


# ---------------------------------------------------------------------------
# Chat / Agent
# ---------------------------------------------------------------------------

@app.post("/sessions/{session_id}/chat", response_model=ChatResponse, tags=["Agent"])
async def chat(
    session_id: str,
    body: ChatRequest,
    session: AgentSession = Depends(_get_session),
    x_user_role: str = Header(default="analyst"),
    x_user_name: str = Header(default="user"),
):
    """
    Send a message to the agent. The agent may call tools internally.
    Returns Claude's final text response and updated conversation history.
    """
    _require_role("viewer", x_user_role)
    result: AgentTurnResult = await run_agent_turn(
        session=session,
        user_message=body.message,
        history=body.history,
        approved_by=x_user_name,
    )
    if result.error and result.error != "max_iterations_reached":
        raise HTTPException(status_code=500, detail=result.error)

    return ChatResponse(
        text=result.text,
        tool_calls_made=result.tool_calls_made,
        new_history=result.new_history,
    )


# ---------------------------------------------------------------------------
# Issues
# ---------------------------------------------------------------------------

@app.get("/sessions/{session_id}/issues", tags=["Issues"])
def list_issues(
    session_id: str,
    status: str | None = None,
    category: str | None = None,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("viewer", x_user_role)),
):
    """List all issues for this session, with optional status/category filters."""
    bulk_threshold = float(os.getenv("DQ_BULK_APPROVE_THRESHOLD", "0.95"))
    issues = list(session.issues.values())

    if status:
        issues = [i for i in issues if i.status.value.upper() == status.upper()]
    if category:
        issues = [i for i in issues if i.category.value.upper() == category.upper()]

    return {
        "dataset_id": session.dataset_meta.dataset_id if session.dataset_meta else None,
        "total": len(issues),
        "issues": [
            IssueListItem(
                issue_id=i.issue_id,
                category=i.category.value,
                severity=i.severity.value,
                status=i.status.value,
                confidence=i.confidence,
                description=i.description,
                affected_columns=i.affected_columns,
                is_actionable=i.is_actionable(),
                can_bulk_approve=i.can_bulk_approve(bulk_threshold),
            ).model_dump()
            for i in issues
        ],
    }


@app.get("/sessions/{session_id}/issues/{issue_id}", tags=["Issues"])
def get_issue(
    session_id: str,
    issue_id: str,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("viewer", x_user_role)),
):
    """Get full details for a single issue."""
    issue = _get_issue(session, issue_id)
    pa = issue.proposed_action
    return {
        "issue_id": issue.issue_id,
        "dataset_id": issue.dataset_id,
        "category": issue.category.value,
        "severity": issue.severity.value,
        "issue_status": issue.status.value,
        "confidence": issue.confidence,
        "description": issue.description,
        "affected_columns": issue.affected_columns,
        "affected_row_count": len(issue.affected_row_indices),
        "raw_values": issue.raw_values,
        "is_actionable": issue.is_actionable(),
        "proposed_action": {
            "action_type": pa.action_type.value,
            "target_column": pa.target_column,
            "canonical_value": pa.canonical_value
                if not isinstance(pa.canonical_value, dict)
                else "<see entity resolution canonical record>",
            "retain_row_index": pa.retain_row_index,
            "retire_row_indices": pa.retire_row_indices,
            "rationale": pa.rationale,
        } if pa else None,
        "detected_at": issue.detected_at.isoformat(),
        "approved_by": issue.approved_by,
        "rejected_by": issue.rejected_by,
        "reviewer_note": issue.reviewer_note,
        "executed_at": issue.executed_at.isoformat() if issue.executed_at else None,
    }


# ---------------------------------------------------------------------------
# Approval / Rejection
# ---------------------------------------------------------------------------

@app.post("/sessions/{session_id}/issues/{issue_id}/approve", tags=["Approval"])
def approve_issue(
    session_id: str,
    issue_id: str,
    body: ApproveRequest,
    session: AgentSession = Depends(_get_session),
    x_user_role: str = Header(default="analyst"),
):
    """
    Approve a single issue. Moves it to PENDING_APPROVAL → APPROVED.

    - HIGH/CRITICAL issues require Senior Analyst or above.
    - LOW/MEDIUM issues require Analyst or above.
    """
    issue = _get_issue(session, issue_id)

    # Role enforcement per severity
    min_role = "senior analyst" if issue.severity.value in ("HIGH", "CRITICAL") else "analyst"
    _require_role(min_role, x_user_role)

    if issue.status not in (IssueStatus.OPEN, IssueStatus.PENDING_APPROVAL):
        raise HTTPException(
            status_code=409,
            detail=f"Issue is in status '{issue.status.value}' and cannot be approved.",
        )

    issue.status = IssueStatus.APPROVED
    issue.approved_by = body.approved_by
    issue.approved_at = datetime.now(timezone.utc)
    issue.reviewer_note = body.note or issue.reviewer_note

    return {
        "issue_id": issue_id,
        "issue_status": issue.status.value,
        "approved_by": issue.approved_by,
        "approved_at": issue.approved_at.isoformat(),
    }


@app.post("/sessions/{session_id}/issues/{issue_id}/reject", tags=["Approval"])
def reject_issue(
    session_id: str,
    issue_id: str,
    body: RejectRequest,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("analyst", x_user_role)),
):
    """Reject an issue. Records the decision in the audit log."""
    issue = _get_issue(session, issue_id)
    entry = record_rejection(
        issue=issue,
        rejected_by=body.rejected_by,
        note=body.note,
        audit_log=session.audit_log,
    )
    return {
        "issue_id": issue_id,
        "issue_status": issue.status.value,
        "rejected_by": issue.rejected_by,
        "rejected_at": issue.rejected_at.isoformat() if issue.rejected_at else None,
        "audit_entry_id": entry.entry_id,
    }


@app.post("/sessions/{session_id}/approve-all", tags=["Approval"])
def bulk_approve(
    session_id: str,
    body: BulkApproveRequest,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("senior analyst", x_user_role)),
):
    """
    Bulk-approve all OPEN/PENDING issues that meet the confidence threshold
    AND are eligible for bulk approval (excludes DEDUPLICATION/UNIQUENESS).
    Requires Senior Analyst role.
    """
    threshold = body.confidence_threshold
    eligible = [
        i for i in session.issues.values()
        if i.status in (IssueStatus.OPEN, IssueStatus.PENDING_APPROVAL)
        and i.can_bulk_approve(threshold)
    ]

    now = datetime.now(timezone.utc)
    for issue in eligible:
        issue.status = IssueStatus.APPROVED
        issue.approved_by = body.approved_by
        issue.approved_at = now

    return {
        "approved_count": len(eligible),
        "approved_by": body.approved_by,
        "confidence_threshold": threshold,
        "issue_ids": [i.issue_id for i in eligible],
    }


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

@app.post("/sessions/{session_id}/execute", tags=["Execute"])
def execute_approved(
    session_id: str,
    approved_by: str,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("analyst", x_user_role)),
):
    """
    Execute all APPROVED issues in the correct order.
    Updates the working DataFrame. Returns a per-issue execution summary.
    """
    if session.dataframe is None:
        raise HTTPException(status_code=422, detail="No dataset loaded in this session.")

    issues = list(session.issues.values())
    bulk = execute_all_approved(
        issues=issues,
        df=session.dataframe,
        approved_by=approved_by,
        audit_log=session.audit_log,
    )

    if bulk.clean_df is not None:
        session.dataframe = bulk.clean_df

    return {
        "total": bulk.total,
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
    }


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

@app.post("/sessions/{session_id}/export/file", tags=["Export"])
def export_file(
    session_id: str,
    body: ExportFileRequest,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("analyst", x_user_role)),
):
    """
    Export the clean dataset to a new file (CSV or Excel).
    Returns a download URL. Only valid for file-upload (Mode A) sessions.
    """
    if session.dataframe is None:
        raise HTTPException(status_code=422, detail="No dataset loaded.")

    result = export_clean_file(
        df=session.dataframe,
        meta=session.dataset_meta,
        output_filename=body.output_filename,
        output_dir=OUTPUT_DIR,
        exported_by=body.exported_by,
        audit_log=session.audit_log,
    )

    if not result.success:
        raise HTTPException(status_code=422, detail=result.error)

    return {
        "output_path": result.output_path,
        "output_filename": Path(result.output_path).name,
        "row_count": result.row_count,
        "column_count": result.column_count,
        "download_url": f"/download/{Path(result.output_path).name}",
        "summary": result.summary,
    }


@app.post("/sessions/{session_id}/export/table", tags=["Export"])
def export_table(
    session_id: str,
    body: ExportTableRequest,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("analyst", x_user_role)),
):
    """
    Export the clean dataset to a new table in the source database.
    Only valid for live DB connection (Mode B) sessions.
    """
    if session.dataframe is None:
        raise HTTPException(status_code=422, detail="No dataset loaded.")
    if not session.connection_string:
        raise HTTPException(
            status_code=422,
            detail="No database connection in session. Use /ingest/db first.",
        )

    result = export_clean_table(
        df=session.dataframe,
        meta=session.dataset_meta,
        connection_string=session.connection_string,
        output_table=body.output_table_name,
        if_exists=body.if_exists,
        exported_by=body.exported_by,
        audit_log=session.audit_log,
    )

    if not result.success:
        raise HTTPException(status_code=422, detail=result.error)

    return {
        "output_table": result.output_table,
        "row_count": result.row_count,
        "column_count": result.column_count,
        "summary": result.summary,
    }


@app.get("/download/{filename}", tags=["Export"])
def download_file(filename: str):
    """Serve a previously exported clean file for download."""
    file_path = OUTPUT_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found.")
    # Security: ensure the resolved path is inside OUTPUT_DIR
    try:
        file_path.resolve().relative_to(OUTPUT_DIR.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid filename.")
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="application/octet-stream",
    )


# ---------------------------------------------------------------------------
# Scorecard
# ---------------------------------------------------------------------------

@app.get("/sessions/{session_id}/scorecard", tags=["Reports"])
def get_scorecard(
    session_id: str,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("viewer", x_user_role)),
):
    """Return a DQ scorecard for the current session."""
    if session.dataset_meta is None:
        raise HTTPException(status_code=422, detail="No dataset loaded.")

    issues = list(session.issues.values())
    total_rows = session.dataset_meta.row_count

    all_affected: set[int] = set()
    for issue in issues:
        if issue.severity.value in ("HIGH", "CRITICAL"):
            all_affected.update(issue.affected_row_indices)

    clean_rows = max(0, total_rows - len(all_affected))
    score = round((clean_rows / total_rows) * 100, 1) if total_rows > 0 else 100.0

    by_category: dict[str, dict] = {}
    for issue in issues:
        cat = issue.category.value
        if cat not in by_category:
            by_category[cat] = {"total": 0, "high_critical": 0, "affected_rows": set()}
        by_category[cat]["total"] += 1
        if issue.severity.value in ("HIGH", "CRITICAL"):
            by_category[cat]["high_critical"] += 1
        by_category[cat]["affected_rows"].update(issue.affected_row_indices)

    return {
        "dataset_id": session.dataset_meta.dataset_id,
        "overall_score": score,
        "total_issues": len(issues),
        "rows_with_issues": len(all_affected),
        "total_rows": total_rows,
        "severity_breakdown": {
            "CRITICAL": sum(1 for i in issues if i.severity.value == "CRITICAL"),
            "HIGH":     sum(1 for i in issues if i.severity.value == "HIGH"),
            "MEDIUM":   sum(1 for i in issues if i.severity.value == "MEDIUM"),
            "LOW":      sum(1 for i in issues if i.severity.value == "LOW"),
        },
        "status_breakdown": {
            "OPEN":             sum(1 for i in issues if i.status.value == "OPEN"),
            "PENDING_APPROVAL": sum(1 for i in issues if i.status.value == "PENDING_APPROVAL"),
            "APPROVED":         sum(1 for i in issues if i.status.value == "APPROVED"),
            "REJECTED":         sum(1 for i in issues if i.status.value == "REJECTED"),
            "EXECUTED":         sum(1 for i in issues if i.status.value == "EXECUTED"),
        },
        "by_category": {
            cat: {
                "total_issues": v["total"],
                "high_critical_issues": v["high_critical"],
                "affected_row_count": len(v["affected_rows"]),
            }
            for cat, v in by_category.items()
        },
    }


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

@app.get("/sessions/{session_id}/audit", tags=["Reports"])
def get_audit_log(
    session_id: str,
    session: AgentSession = Depends(_get_session),
    _role: str = Depends(lambda x_user_role=Header(default="analyst"): _require_role("viewer", x_user_role)),
):
    """Return all audit log entries for this session, newest first."""
    entries = sorted(
        session.audit_log.entries(),
        key=lambda e: e.timestamp,
        reverse=True,
    )
    return {
        "session_id": session_id,
        "total": len(entries),
        "entries": [
            AuditEntryResponse(
                entry_id=e.entry_id,
                timestamp=e.timestamp,
                issue_id=e.issue_id,
                action_type=e.action_type,
                actor=e.actor,
                success=e.success,
                notes=e.notes,
                affected_row_count=len(e.affected_row_indices),
            ).model_dump()
            for e in entries
        ],
    }