"""
backend/models/issue.py

The Issue is the atomic unit of the DQ agent's output. Every problem found by
the quality check engine produces exactly one Issue. Issues flow through a
defined lifecycle:

    OPEN → PENDING_APPROVAL → APPROVED → EXECUTED
                           ↘ REJECTED

An Issue is never deleted — only its status changes. The audit log records
every transition with who made it and when.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class IssueCategory(str, Enum):
    COMPLETENESS = "COMPLETENESS"           # Null / blank required fields
    UNIQUENESS = "UNIQUENESS"               # Exact duplicate rows
    DEDUPLICATION = "DEDUPLICATION"         # Fuzzy / entity-resolution duplicates
    FORMAT_VALIDITY = "FORMAT_VALIDITY"     # Wrong format (date, currency, phone, etc.)
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"  # FK / lookup value not in reference set
    CONSISTENCY = "CONSISTENCY"             # Conflicting values for the same entity
    OUTLIER = "OUTLIER"                     # Statistical anomaly (> N σ from mean)
    DOMAIN_RULE = "DOMAIN_RULE"             # Business rule violation (negative qty, future date, etc.)


class IssueSeverity(str, Enum):
    LOW = "LOW"         # Cosmetic / informational — downstream impact unlikely
    MEDIUM = "MEDIUM"   # May cause reporting errors or join failures
    HIGH = "HIGH"       # Will cause downstream system errors or data loss
    CRITICAL = "CRITICAL"  # Blocks export until resolved


class IssueStatus(str, Enum):
    OPEN = "OPEN"                       # Found, not yet reviewed
    PENDING_APPROVAL = "PENDING_APPROVAL"  # Proposal staged, awaiting human decision
    APPROVED = "APPROVED"               # Human approved the proposed fix
    REJECTED = "REJECTED"               # Human rejected — issue noted, no change made
    EXECUTED = "EXECUTED"               # Approved fix has been applied to the dataset


# ---------------------------------------------------------------------------
# Proposed action types
# ---------------------------------------------------------------------------


class ActionType(str, Enum):
    SET_VALUE = "SET_VALUE"         # Replace a cell value with a canonical value
    CLEAR_VALUE = "CLEAR_VALUE"     # Set a cell to null/empty
    MERGE_ROWS = "MERGE_ROWS"       # Deduplicate — retain one row, retire others
    DROP_ROW = "DROP_ROW"           # Remove a row entirely
    RETYPE_COLUMN = "RETYPE_COLUMN" # Cast a column to the correct dtype
    FLAG_ONLY = "FLAG_ONLY"         # No automatic fix — human must decide manually


class ProposedAction(BaseModel):
    """
    A structured, machine-executable description of the fix Claude proposes
    for a given issue. The Cleansing Engine reads this to apply changes.

    Only populated when confidence >= DQ_CONFIDENCE_THRESHOLD. Below that
    threshold, action_type is FLAG_ONLY and the human must specify the fix.
    """

    action_type: ActionType
    # SET_VALUE / CLEAR_VALUE
    target_column: str | None = None
    target_row_indices: list[int] = Field(default_factory=list)
    canonical_value: Any = None           # The replacement value

    # MERGE_ROWS / DEDUPLICATION
    retain_row_index: int | None = None   # Row index to keep
    retire_row_indices: list[int] = Field(default_factory=list)  # Row indices to remove

    # DROP_ROW
    drop_row_indices: list[int] = Field(default_factory=list)

    # RETYPE_COLUMN
    target_dtype: str | None = None       # e.g. "float64", "datetime64[ns]"

    # Human-readable rationale for the proposed action
    rationale: str = ""


# ---------------------------------------------------------------------------
# Core Issue model
# ---------------------------------------------------------------------------


class Issue(BaseModel):
    """
    A single data quality finding produced by the quality check engine.

    Immutable fields (set at creation): issue_id, dataset_id, category,
    severity, affected_row_indices, affected_columns, description, confidence,
    raw_values, proposed_action.

    Mutable fields (updated through lifecycle): status, approved_by,
    approved_at, rejected_by, rejected_at, reviewer_note, executed_at.
    """

    # --- Identity ---
    issue_id: str = Field(default_factory=lambda: f"DQ-{str(uuid.uuid4())[:8].upper()}")
    dataset_id: str

    # --- Classification ---
    category: IssueCategory
    severity: IssueSeverity
    status: IssueStatus = IssueStatus.OPEN

    # --- Location ---
    # Row indices into the DataFrame (0-based, matching pandas iloc)
    affected_row_indices: list[int] = Field(default_factory=list)
    # Column names involved in this issue
    affected_columns: list[str] = Field(default_factory=list)

    # --- Description ---
    description: str                  # Plain English, suitable for a non-technical analyst
    confidence: float = 1.0           # 0.0–1.0; below threshold → FLAG_ONLY

    # Raw before-values for the affected cells, for display and audit purposes
    # Format: {column_name: [value_for_row_0, value_for_row_1, ...]}
    raw_values: dict[str, list[Any]] = Field(default_factory=dict)

    # --- Fix proposal ---
    proposed_action: ProposedAction | None = None

    # --- Lifecycle timestamps and actors ---
    detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    approved_by: str | None = None
    approved_at: datetime | None = None
    rejected_by: str | None = None
    rejected_at: datetime | None = None
    reviewer_note: str | None = None
    executed_at: datetime | None = None

    # --- Helpers ---

    def is_actionable(self) -> bool:
        """True if the issue has a concrete proposed fix (not FLAG_ONLY)."""
        return (
            self.proposed_action is not None
            and self.proposed_action.action_type != ActionType.FLAG_ONLY
        )

    def can_bulk_approve(self, bulk_threshold: float) -> bool:
        """
        True if this issue is eligible for bulk approval.
        Deduplication/merge issues are always excluded from bulk approval
        regardless of confidence, per CLAUDE.md design decision #8.
        """
        if self.category in (IssueCategory.DEDUPLICATION, IssueCategory.UNIQUENESS):
            return False
        return self.confidence >= bulk_threshold


# ---------------------------------------------------------------------------
# Aggregate report returned by run_quality_checks
# ---------------------------------------------------------------------------


class DQReport(BaseModel):
    """
    The full output of a single quality-check run against a dataset.
    Returned by run_quality_checks() and stored in the issue registry.
    """

    dataset_id: str
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None

    issues: list[Issue] = Field(default_factory=list)

    # Counts by severity — populated by finalize()
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    total_count: int = 0

    # Categories that were actually run
    categories_checked: list[str] = Field(default_factory=list)

    def finalize(self) -> "DQReport":
        """Compute summary counts and set completed_at. Call after all checks run."""
        self.completed_at = datetime.now(timezone.utc)
        self.total_count = len(self.issues)
        self.critical_count = sum(1 for i in self.issues if i.severity == IssueSeverity.CRITICAL)
        self.high_count = sum(1 for i in self.issues if i.severity == IssueSeverity.HIGH)
        self.medium_count = sum(1 for i in self.issues if i.severity == IssueSeverity.MEDIUM)
        self.low_count = sum(1 for i in self.issues if i.severity == IssueSeverity.LOW)
        return self

    def issues_by_category(self) -> dict[str, list[Issue]]:
        result: dict[str, list[Issue]] = {}
        for issue in self.issues:
            result.setdefault(issue.category.value, []).append(issue)
        return result

    def summary_text(self) -> str:
        cats = self.issues_by_category()
        lines = [
            f"DQ check complete for dataset {self.dataset_id}.",
            f"Found {self.total_count} issue(s): "
            f"{self.critical_count} CRITICAL, {self.high_count} HIGH, "
            f"{self.medium_count} MEDIUM, {self.low_count} LOW.",
        ]
        for cat, issues in sorted(cats.items()):
            lines.append(f"  {cat}: {len(issues)} issue(s)")
        return "\n".join(lines)
