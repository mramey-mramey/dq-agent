"""
backend/tools/cleanse.py

Cleansing Engine — the only place in the codebase that modifies a DataFrame.

Entry points:
    execute_approved_cleanse(issue, df, approved_by) -> CleanseResult
    execute_all_approved(issues, df, approved_by)    -> BulkCleanseResult

Design constraints (from CLAUDE.md):
    - APPROVAL IS ENFORCED SERVER-SIDE. This module checks issue.status ==
      APPROVED before touching any data. The agent prompt alone is not
      sufficient authorisation — the gate is here in code.
    - FLAG_ONLY actions are never executable. Attempting to execute one
      returns a failed CleanseResult; the DataFrame is untouched.
    - Every execution (success or failure) writes an AuditEntry. The audit
      log is append-only — entries are never deleted or modified.
    - All mutations work on a COPY of the DataFrame. The caller replaces
      their working copy with the returned clean_df; the original is never
      passed by reference in a way that allows silent mutation.
    - MERGE_ROWS with a canonical_value dict applies per-column overrides
      to the retained row before retiring the others.
    - Execution order matters for MERGE_ROWS: row retirements are batched
      and applied after all SET_VALUE / CLEAR_VALUE / RETYPE_COLUMN ops so
      that column fixes on rows-to-be-retired don't interfere.

Supported ActionTypes:
    SET_VALUE       — set target_column to canonical_value for target_row_indices
    CLEAR_VALUE     — set target_column to pd.NA for target_row_indices
    MERGE_ROWS      — keep retain_row_index, apply canonical_value overrides,
                      drop retire_row_indices
    DROP_ROW        — drop drop_row_indices entirely
    RETYPE_COLUMN   — cast target_column to target_dtype
    FLAG_ONLY       — not executable; returns error
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

from backend.models.issue import ActionType, Issue, IssueStatus, ProposedAction

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Audit log entry
# ---------------------------------------------------------------------------


class AuditEntry(BaseModel):
    """
    An immutable record of a single cleansing action (or rejection).
    Written for every call to execute_approved_cleanse, whether it succeeds
    or fails, and for every rejection recorded via record_rejection().

    The audit log is the source of truth for compliance — it captures the
    before-state, the after-state, who authorised the change, and when.
    """

    entry_id: str = Field(
        default_factory=lambda: f"AUD-{__import__('uuid').uuid4().hex[:8].upper()}"
    )
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    dataset_id: str
    issue_id: str
    action_type: str          # ActionType.value string

    # Who authorised / rejected
    actor: str                # username or "SYSTEM"
    actor_role: str = ""      # optional role label (Analyst, Senior Analyst, etc.)

    # What changed
    affected_columns: list[str] = Field(default_factory=list)
    affected_row_indices: list[int] = Field(default_factory=list)
    before_values: dict[str, Any] = Field(default_factory=dict)
    after_values: dict[str, Any] = Field(default_factory=dict)

    # Outcome
    success: bool
    notes: str = ""           # Human-supplied reviewer note or error message


class AuditLog:
    """
    In-memory append-only audit log for a single session.
    In production this would persist to the database via SQLAlchemy.
    """

    def __init__(self) -> None:
        self._entries: list[AuditEntry] = []

    def append(self, entry: AuditEntry) -> None:
        self._entries.append(entry)

    def entries(self) -> list[AuditEntry]:
        """Return a snapshot of all entries (read-only copy)."""
        return list(self._entries)

    def entries_for_dataset(self, dataset_id: str) -> list[AuditEntry]:
        return [e for e in self._entries if e.dataset_id == dataset_id]

    def entries_for_issue(self, issue_id: str) -> list[AuditEntry]:
        return [e for e in self._entries if e.issue_id == issue_id]

    def __len__(self) -> int:
        return len(self._entries)

    def __bool__(self) -> bool:
        # Always truthy — don't conflate an empty log with None.
        # Callers use `if audit_log:` to check presence, not emptiness.
        return True


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


class CleanseResult(BaseModel):
    """Result of a single execute_approved_cleanse call."""

    model_config = {"arbitrary_types_allowed": True}

    success: bool
    issue_id: str
    action_type: str
    rows_affected: int = 0
    clean_df: pd.DataFrame | None = None   # Updated DataFrame; None on failure
    audit_entry: AuditEntry | None = None
    error: str | None = None


class BulkCleanseResult(BaseModel):
    """Aggregated result of execute_all_approved across multiple issues."""

    model_config = {"arbitrary_types_allowed": True}

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0          # Issues not in APPROVED status
    results: list[CleanseResult] = Field(default_factory=list)
    clean_df: pd.DataFrame | None = None   # Final DataFrame after all cleanses
    audit_entries: list[AuditEntry] = Field(default_factory=list)

    @property
    def all_succeeded(self) -> bool:
        return self.failed == 0 and self.succeeded > 0


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def execute_approved_cleanse(
    issue: Issue,
    df: pd.DataFrame,
    approved_by: str,
    *,
    actor_role: str = "",
    audit_log: AuditLog | None = None,
) -> CleanseResult:
    """
    Apply the fix described in issue.proposed_action to df.

    APPROVAL GATE: issue.status must be APPROVED. Any other status
    (including PENDING_APPROVAL) returns a failed result immediately —
    the DataFrame is never touched.

    Args:
        issue:       The Issue to execute. Must be in APPROVED status.
        df:          Working DataFrame. A copy is made before any mutation.
        approved_by: Username of the approver. Written to the audit log.
        actor_role:  Optional role label for the audit log.
        audit_log:   If provided, the AuditEntry is appended here.

    Returns:
        CleanseResult with the updated DataFrame on success, or an error
        message and the original DataFrame on failure.
    """
    # --- Server-side approval gate (CLAUDE.md design decision #1) ---
    if issue.status != IssueStatus.APPROVED:
        msg = (
            f"Issue {issue.issue_id} cannot be executed: "
            f"status is '{issue.status.value}', expected 'APPROVED'."
        )
        logger.warning(msg)
        entry = _make_audit_entry(
            issue=issue,
            actor=approved_by,
            actor_role=actor_role,
            success=False,
            before_values={},
            after_values={},
            rows_affected=[],
            notes=msg,
        )
        if audit_log:
            audit_log.append(entry)
        return CleanseResult(
            success=False,
            issue_id=issue.issue_id,
            action_type=issue.proposed_action.action_type.value if issue.proposed_action else "NONE",
            error=msg,
            audit_entry=entry,
        )

    pa = issue.proposed_action

    # --- FLAG_ONLY is never executable ---
    if pa is None or pa.action_type == ActionType.FLAG_ONLY:
        msg = (
            f"Issue {issue.issue_id} has action_type FLAG_ONLY and cannot be "
            f"automatically executed. The analyst must apply this fix manually."
        )
        logger.warning(msg)
        entry = _make_audit_entry(
            issue=issue,
            actor=approved_by,
            actor_role=actor_role,
            success=False,
            before_values={},
            after_values={},
            rows_affected=[],
            notes=msg,
        )
        if audit_log:
            audit_log.append(entry)
        return CleanseResult(
            success=False,
            issue_id=issue.issue_id,
            action_type=pa.action_type.value if pa else "NONE",
            error=msg,
            audit_entry=entry,
        )

    # --- Snapshot before-values for audit ---
    before_values = _snapshot_before(df, pa)

    # --- Execute on a copy ---
    clean_df = df.copy()
    try:
        rows_affected = _dispatch(clean_df, pa)
    except Exception as exc:
        msg = f"Execution error on issue {issue.issue_id}: {exc}"
        logger.exception(msg)
        entry = _make_audit_entry(
            issue=issue,
            actor=approved_by,
            actor_role=actor_role,
            success=False,
            before_values=before_values,
            after_values={},
            rows_affected=[],
            notes=msg,
        )
        if audit_log:
            audit_log.append(entry)
        return CleanseResult(
            success=False,
            issue_id=issue.issue_id,
            action_type=pa.action_type.value,
            error=msg,
            audit_entry=entry,
        )

    # --- Snapshot after-values ---
    after_values = _snapshot_after(clean_df, pa, rows_affected)

    # --- Mark issue as EXECUTED ---
    issue.status = IssueStatus.EXECUTED
    issue.executed_at = datetime.now(timezone.utc)

    entry = _make_audit_entry(
        issue=issue,
        actor=approved_by,
        actor_role=actor_role,
        success=True,
        before_values=before_values,
        after_values=after_values,
        rows_affected=rows_affected,
        notes=issue.reviewer_note or "",
    )
    if audit_log:
        audit_log.append(entry)

    logger.info(
        "Executed %s on issue %s (%d row(s) affected) by %s",
        pa.action_type.value,
        issue.issue_id,
        len(rows_affected),
        approved_by,
    )

    return CleanseResult(
        success=True,
        issue_id=issue.issue_id,
        action_type=pa.action_type.value,
        rows_affected=len(rows_affected),
        clean_df=clean_df,
        audit_entry=entry,
    )


def execute_all_approved(
    issues: list[Issue],
    df: pd.DataFrame,
    approved_by: str,
    *,
    actor_role: str = "",
    audit_log: AuditLog | None = None,
) -> BulkCleanseResult:
    """
    Execute all APPROVED, actionable issues against df in a safe order.

    Execution order (important for correctness):
        1. RETYPE_COLUMN   — cast types first so SET_VALUE assigns the right dtype
        2. SET_VALUE        — cell-level replacements
        3. CLEAR_VALUE      — nullifications
        4. MERGE_ROWS       — row retirements (after column fixes are applied)
        5. DROP_ROW         — unconditional row removal

    Issues that are not APPROVED, or have FLAG_ONLY actions, are counted as
    skipped and do not affect the result DataFrame.

    Args:
        issues:      Full list of Issue objects for this dataset run.
        df:          Working DataFrame. Not modified in place.
        approved_by: Username applied to all audit entries.
        actor_role:  Optional role label.
        audit_log:   If provided, all AuditEntry objects are appended here.

    Returns:
        BulkCleanseResult with the final clean DataFrame and per-issue results.
    """
    _ORDER = [
        ActionType.RETYPE_COLUMN,
        ActionType.SET_VALUE,
        ActionType.CLEAR_VALUE,
        ActionType.MERGE_ROWS,
        ActionType.DROP_ROW,
    ]

    # Partition issues by executability
    executable: list[Issue] = []
    skipped_issues: list[Issue] = []

    for issue in issues:
        if issue.status != IssueStatus.APPROVED:
            skipped_issues.append(issue)
            continue
        pa = issue.proposed_action
        if pa is None or pa.action_type == ActionType.FLAG_ONLY:
            skipped_issues.append(issue)
            continue
        executable.append(issue)

    # Sort by execution order
    order_map = {at: i for i, at in enumerate(_ORDER)}
    executable.sort(
        key=lambda iss: order_map.get(iss.proposed_action.action_type, 99)
    )

    current_df = df.copy()
    results: list[CleanseResult] = []
    all_entries: list[AuditEntry] = []

    for issue in executable:
        result = execute_approved_cleanse(
            issue=issue,
            df=current_df,
            approved_by=approved_by,
            actor_role=actor_role,
            audit_log=audit_log,
        )
        results.append(result)
        if result.audit_entry:
            all_entries.append(result.audit_entry)
        if result.success and result.clean_df is not None:
            current_df = result.clean_df

    succeeded = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)

    return BulkCleanseResult(
        total=len(issues),
        succeeded=succeeded,
        failed=failed,
        skipped=len(skipped_issues),
        results=results,
        clean_df=current_df,
        audit_entries=all_entries,
    )


def record_rejection(
    issue: Issue,
    rejected_by: str,
    note: str = "",
    *,
    actor_role: str = "",
    audit_log: AuditLog | None = None,
) -> AuditEntry:
    """
    Record a rejection decision in the audit log.
    Updates issue.status to REJECTED and writes an AuditEntry.

    Args:
        issue:       The Issue being rejected.
        rejected_by: Username of the reviewer.
        note:        Optional reason for rejection.
        actor_role:  Optional role label.
        audit_log:   If provided, the AuditEntry is appended here.

    Returns:
        The AuditEntry written.
    """
    issue.status = IssueStatus.REJECTED
    issue.rejected_by = rejected_by
    issue.rejected_at = datetime.now(timezone.utc)
    issue.reviewer_note = note

    pa = issue.proposed_action
    entry = AuditEntry(
        dataset_id=issue.dataset_id,
        issue_id=issue.issue_id,
        action_type=pa.action_type.value if pa else "NONE",
        actor=rejected_by,
        actor_role=actor_role,
        affected_columns=issue.affected_columns,
        affected_row_indices=issue.affected_row_indices,
        before_values={},
        after_values={},
        success=False,
        notes=f"REJECTED. {note}".strip(),
    )
    if audit_log:
        audit_log.append(entry)
    logger.info("Issue %s rejected by %s.", issue.issue_id, rejected_by)
    return entry


# ---------------------------------------------------------------------------
# Action dispatcher
# ---------------------------------------------------------------------------


def _dispatch(df: pd.DataFrame, pa: ProposedAction) -> list[int]:
    """
    Apply the proposed action to df IN PLACE and return the list of
    row indices that were actually modified.
    """
    if pa.action_type == ActionType.SET_VALUE:
        return _apply_set_value(df, pa)

    if pa.action_type == ActionType.CLEAR_VALUE:
        return _apply_clear_value(df, pa)

    if pa.action_type == ActionType.MERGE_ROWS:
        return _apply_merge_rows(df, pa)

    if pa.action_type == ActionType.DROP_ROW:
        return _apply_drop_row(df, pa)

    if pa.action_type == ActionType.RETYPE_COLUMN:
        return _apply_retype_column(df, pa)

    raise ValueError(f"Unsupported action_type: {pa.action_type}")


# ---------------------------------------------------------------------------
# Action implementations
# ---------------------------------------------------------------------------


def _apply_set_value(df: pd.DataFrame, pa: ProposedAction) -> list[int]:
    """
    SET_VALUE: replace target_column with canonical_value for all
    target_row_indices that exist in df.
    """
    if not pa.target_column:
        raise ValueError("SET_VALUE requires target_column.")
    if pa.target_column not in df.columns:
        raise ValueError(f"Column '{pa.target_column}' not found in DataFrame.")

    valid_indices = [idx for idx in pa.target_row_indices if idx in df.index]
    if not valid_indices:
        logger.warning("SET_VALUE: none of %s are valid indices.", pa.target_row_indices)
        return []

    # Strip currency noise from numeric-looking canonical values if the column
    # is already numeric — avoids dtype conflicts on assignment
    value = _coerce_value(pa.canonical_value, df[pa.target_column].dtype)
    df.loc[valid_indices, pa.target_column] = value
    return valid_indices


def _apply_clear_value(df: pd.DataFrame, pa: ProposedAction) -> list[int]:
    """
    CLEAR_VALUE: set target_column to pd.NA for all target_row_indices.
    """
    if not pa.target_column:
        raise ValueError("CLEAR_VALUE requires target_column.")
    if pa.target_column not in df.columns:
        raise ValueError(f"Column '{pa.target_column}' not found in DataFrame.")

    valid_indices = [idx for idx in pa.target_row_indices if idx in df.index]
    if not valid_indices:
        return []

    df.loc[valid_indices, pa.target_column] = pd.NA
    return valid_indices


def _apply_merge_rows(df: pd.DataFrame, pa: ProposedAction) -> list[int]:
    """
    MERGE_ROWS:
        1. If canonical_value is a dict, apply per-column overrides to the
           retain row (allows entity_resolution canonical record to be written).
        2. If canonical_value is a scalar and target_column is set, apply it
           to that column on the retain row (quality_checks dedup style).
        3. Drop all retire_row_indices from df in place.

    Returns the combined list of affected row indices (retain + retired).
    """
    if pa.retain_row_index is None:
        raise ValueError("MERGE_ROWS requires retain_row_index.")
    if pa.retain_row_index not in df.index:
        raise ValueError(
            f"retain_row_index {pa.retain_row_index} not found in DataFrame."
        )

    all_affected: list[int] = [pa.retain_row_index]

    # --- Apply canonical overrides to the retained row ---
    if isinstance(pa.canonical_value, dict):
        for col, val in pa.canonical_value.items():
            if col in df.columns:
                coerced = _coerce_value(val, df[col].dtype)
                df.loc[pa.retain_row_index, col] = coerced
    elif pa.canonical_value is not None and pa.target_column:
        if pa.target_column in df.columns:
            coerced = _coerce_value(pa.canonical_value, df[pa.target_column].dtype)
            df.loc[pa.retain_row_index, pa.target_column] = coerced

    # --- Drop retired rows ---
    valid_retire = [idx for idx in pa.retire_row_indices if idx in df.index]
    if valid_retire:
        df.drop(index=valid_retire, inplace=True)
        all_affected.extend(valid_retire)

    return all_affected


def _apply_drop_row(df: pd.DataFrame, pa: ProposedAction) -> list[int]:
    """
    DROP_ROW: remove drop_row_indices from df unconditionally.
    """
    valid_indices = [idx for idx in pa.drop_row_indices if idx in df.index]
    if not valid_indices:
        logger.warning("DROP_ROW: none of %s are valid indices.", pa.drop_row_indices)
        return []

    df.drop(index=valid_indices, inplace=True)
    return valid_indices


def _apply_retype_column(df: pd.DataFrame, pa: ProposedAction) -> list[int]:
    """
    RETYPE_COLUMN: cast target_column to target_dtype.
    Rows that fail conversion are set to pd.NA (errors='coerce').
    Returns a list containing every row index in the column (all are affected).
    """
    if not pa.target_column:
        raise ValueError("RETYPE_COLUMN requires target_column.")
    if not pa.target_dtype:
        raise ValueError("RETYPE_COLUMN requires target_dtype.")
    if pa.target_column not in df.columns:
        raise ValueError(f"Column '{pa.target_column}' not found in DataFrame.")

    dtype = pa.target_dtype.lower().strip()

    if "float" in dtype or "int" in dtype:
        df[pa.target_column] = pd.to_numeric(df[pa.target_column], errors="coerce")
        if "float" in dtype:
            # Explicit cast to float: to_numeric returns int64 when all values
            # are whole numbers, which would fail a float dtype assertion.
            df[pa.target_column] = df[pa.target_column].astype(float)
    elif "datetime" in dtype or dtype == "date":
        df[pa.target_column] = pd.to_datetime(df[pa.target_column], errors="coerce")
    elif dtype in ("str", "string", "object"):
        df[pa.target_column] = df[pa.target_column].astype(str)
    else:
        # Generic fallback — let pandas try
        try:
            df[pa.target_column] = df[pa.target_column].astype(dtype)
        except Exception as exc:
            raise ValueError(
                f"Cannot cast column '{pa.target_column}' to dtype '{dtype}': {exc}"
            ) from exc

    return list(df.index)


# ---------------------------------------------------------------------------
# Snapshot helpers (before/after values for audit)
# ---------------------------------------------------------------------------


def _snapshot_before(df: pd.DataFrame, pa: ProposedAction) -> dict[str, Any]:
    """Capture current values of the cells about to change."""
    snap: dict[str, Any] = {}

    if pa.action_type in (ActionType.SET_VALUE, ActionType.CLEAR_VALUE):
        col = pa.target_column
        if col and col in df.columns:
            indices = [i for i in pa.target_row_indices if i in df.index]
            snap[col] = {idx: _safe_scalar(df.at[idx, col]) for idx in indices}

    elif pa.action_type == ActionType.MERGE_ROWS:
        all_idx = (
            ([pa.retain_row_index] if pa.retain_row_index in df.index else [])
            + [i for i in pa.retire_row_indices if i in df.index]
        )
        for col in df.columns:
            snap[col] = {idx: _safe_scalar(df.at[idx, col]) for idx in all_idx}

    elif pa.action_type == ActionType.DROP_ROW:
        all_idx = [i for i in pa.drop_row_indices if i in df.index]
        for col in df.columns:
            snap[col] = {idx: _safe_scalar(df.at[idx, col]) for idx in all_idx}

    elif pa.action_type == ActionType.RETYPE_COLUMN:
        col = pa.target_column
        if col and col in df.columns:
            snap[col] = {
                "dtype_before": str(df[col].dtype),
                "sample": df[col].head(3).tolist(),
            }

    return snap


def _snapshot_after(
    df: pd.DataFrame, pa: ProposedAction, rows_affected: list[int]
) -> dict[str, Any]:
    """Capture post-mutation values of the cells that were changed."""
    snap: dict[str, Any] = {}

    if pa.action_type == ActionType.SET_VALUE:
        col = pa.target_column
        if col and col in df.columns:
            indices = [i for i in rows_affected if i in df.index]
            snap[col] = {idx: _safe_scalar(df.at[idx, col]) for idx in indices}

    elif pa.action_type == ActionType.CLEAR_VALUE:
        col = pa.target_column
        if col and col in df.columns:
            indices = [i for i in rows_affected if i in df.index]
            snap[col] = {idx: None for idx in indices}

    elif pa.action_type == ActionType.MERGE_ROWS:
        if pa.retain_row_index in df.index:
            snap["retained_row"] = pa.retain_row_index
        snap["retired_rows"] = pa.retire_row_indices

    elif pa.action_type == ActionType.DROP_ROW:
        snap["dropped_rows"] = rows_affected

    elif pa.action_type == ActionType.RETYPE_COLUMN:
        col = pa.target_column
        if col and col in df.columns:
            snap[col] = {
                "dtype_after": str(df[col].dtype),
                "sample": df[col].head(3).tolist(),
            }

    return snap


# ---------------------------------------------------------------------------
# Audit entry factory
# ---------------------------------------------------------------------------


def _make_audit_entry(
    issue: Issue,
    actor: str,
    actor_role: str,
    success: bool,
    before_values: dict[str, Any],
    after_values: dict[str, Any],
    rows_affected: list[int],
    notes: str,
) -> AuditEntry:
    pa = issue.proposed_action
    return AuditEntry(
        dataset_id=issue.dataset_id,
        issue_id=issue.issue_id,
        action_type=pa.action_type.value if pa else "NONE",
        actor=actor,
        actor_role=actor_role,
        affected_columns=issue.affected_columns,
        affected_row_indices=rows_affected,
        before_values=before_values,
        after_values=after_values,
        success=success,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _coerce_value(value: Any, target_dtype) -> Any:
    """
    Attempt to coerce a canonical value to be compatible with the column dtype.
    Returns the value unchanged if coercion isn't needed or possible.
    """
    if value is None or (isinstance(value, float) and value != value):  # NaN
        return pd.NA

    dtype_str = str(target_dtype).lower()

    if "float" in dtype_str or "int" in dtype_str:
        if isinstance(value, str):
            # Strip common currency noise before numeric coercion
            import re
            cleaned = re.sub(r"[$€£¥,\s]", "", value).strip()
            try:
                return float(cleaned)
            except ValueError:
                return value
        try:
            return float(value) if "float" in dtype_str else int(value)
        except (ValueError, TypeError):
            return value

    return value


def _safe_scalar(value: Any) -> Any:
    """Convert pandas/numpy scalars to plain Python for JSON serialisation."""
    if value is None:
        return None
    try:
        import numpy as np
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
        if isinstance(value, (np.bool_,)):
            return bool(value)
    except ImportError:
        pass
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value