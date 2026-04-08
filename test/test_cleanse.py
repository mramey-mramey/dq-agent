"""
tests/test_cleanse.py

Unit tests for backend/tools/cleanse.py.

Coverage:
    Approval gate:
        - OPEN status rejected
        - PENDING_APPROVAL status rejected
        - REJECTED status rejected
        - EXECUTED status rejected
        - APPROVED status allowed
        - FLAG_ONLY action rejected even when APPROVED
        - DataFrame not modified on gate failure

    SET_VALUE:
        - value written to correct cells
        - other rows untouched
        - invalid row indices ignored (no crash)
        - currency-noise string coerced on numeric column
        - audit entry captures before/after values

    CLEAR_VALUE:
        - cells set to NA
        - other cells untouched
        - audit entry written

    MERGE_ROWS:
        - retired rows removed from DataFrame
        - retain row survives
        - scalar canonical_value applied to target_column on retain row
        - dict canonical_value applies per-column overrides
        - invalid retire indices silently skipped
        - retain index not in df raises ValueError
        - audit entry written with retained/retired info

    DROP_ROW:
        - specified rows removed
        - other rows untouched
        - invalid indices silently skipped
        - audit entry written

    RETYPE_COLUMN:
        - string column cast to float64
        - string column cast to datetime
        - unparseable values become NA
        - audit entry captures dtype before/after

    execute_all_approved:
        - only APPROVED issues executed
        - FLAG_ONLY issues counted as skipped
        - OPEN/REJECTED issues counted as skipped
        - correct execution order (RETYPE before SET_VALUE, MERGE last)
        - DataFrame threaded correctly through chain
        - bulk result counts accurate
        - all_succeeded property

    record_rejection:
        - issue.status set to REJECTED
        - rejected_by / rejected_at populated
        - AuditEntry written with success=False
        - note preserved

    AuditLog:
        - append / entries / len
        - entries_for_dataset filter
        - entries_for_issue filter
        - append-only (no delete/replace)
"""

from __future__ import annotations

import os
import tempfile
from copy import deepcopy
from datetime import datetime, timezone

import pandas as pd
import pytest

os.environ.setdefault("DQ_MAX_RECORDS_PER_RUN", "50000")
os.environ.setdefault("OUTPUT_DIR", tempfile.mkdtemp(prefix="dq_cleanse_test_"))

from backend.models.issue import (
    ActionType,
    Issue,
    IssueCategory,
    IssueSeverity,
    IssueStatus,
    ProposedAction,
)
from backend.tools.cleanse import (
    AuditEntry,
    AuditLog,
    BulkCleanseResult,
    CleanseResult,
    execute_all_approved,
    execute_approved_cleanse,
    record_rejection,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "vendor_id":   ["V-001", "V-002", "V-003", "V-004", "V-005"],
            "vendor_name": ["Acme Corp.", "ACME Corporation", "Globex", "Pendant", "Initech"],
            "amount":      [1000.0, 500.0, 2500.0, 750.0, 3200.0],
            "state":       ["IL", "IL", "IL", "NY", "PA"],
            "payment_terms": ["NET30", "NET30", "NET45", None, "NET60"],
        }
    )


def _approved_issue(
    action_type: ActionType,
    dataset_id: str = "ds-001",
    **pa_kwargs,
) -> Issue:
    """Return an Issue in APPROVED status with the given ProposedAction."""
    pa = ProposedAction(action_type=action_type, **pa_kwargs)
    issue = Issue(
        dataset_id=dataset_id,
        category=IssueCategory.COMPLETENESS,
        severity=IssueSeverity.MEDIUM,
        description="test issue",
        proposed_action=pa,
        status=IssueStatus.APPROVED,
        approved_by="analyst@example.com",
        approved_at=datetime.now(timezone.utc),
    )
    return issue


def _issue_with_status(status: IssueStatus, action_type: ActionType = ActionType.SET_VALUE) -> Issue:
    pa = ProposedAction(
        action_type=action_type,
        target_column="vendor_name",
        target_row_indices=[0],
        canonical_value="Fixed",
    )
    return Issue(
        dataset_id="ds-001",
        category=IssueCategory.COMPLETENESS,
        severity=IssueSeverity.LOW,
        description="test",
        proposed_action=pa,
        status=status,
    )


# ---------------------------------------------------------------------------
# Approval gate
# ---------------------------------------------------------------------------


class TestApprovalGate:
    @pytest.mark.parametrize("status", [
        IssueStatus.OPEN,
        IssueStatus.PENDING_APPROVAL,
        IssueStatus.REJECTED,
        IssueStatus.EXECUTED,
    ])
    def test_non_approved_status_rejected(self, status):
        df = _make_df()
        issue = _issue_with_status(status)
        result = execute_approved_cleanse(issue, df, approved_by="user")
        assert result.success is False
        assert "APPROVED" in result.error

    def test_approved_status_allowed(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Acme Corporation",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True

    def test_flag_only_rejected_even_when_approved(self):
        df = _make_df()
        issue = _approved_issue(ActionType.FLAG_ONLY)
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is False
        assert "FLAG_ONLY" in result.error

    def test_df_unchanged_on_gate_failure(self):
        df = _make_df()
        original = df.copy()
        issue = _issue_with_status(IssueStatus.OPEN)
        execute_approved_cleanse(issue, df, approved_by="user")
        pd.testing.assert_frame_equal(df, original)

    def test_audit_entry_written_on_gate_failure(self):
        df = _make_df()
        log = AuditLog()
        issue = _issue_with_status(IssueStatus.OPEN)
        execute_approved_cleanse(issue, df, approved_by="user", audit_log=log)
        assert len(log) == 1
        assert log.entries()[0].success is False

    def test_issue_status_not_mutated_on_gate_failure(self):
        df = _make_df()
        issue = _issue_with_status(IssueStatus.OPEN)
        execute_approved_cleanse(issue, df, approved_by="user")
        # Should still be OPEN — we didn't change it
        assert issue.status == IssueStatus.OPEN


# ---------------------------------------------------------------------------
# SET_VALUE
# ---------------------------------------------------------------------------


class TestSetValue:
    def test_value_written_to_target_cells(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0, 1],
            canonical_value="Acme Corporation",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert result.clean_df.loc[0, "vendor_name"] == "Acme Corporation"
        assert result.clean_df.loc[1, "vendor_name"] == "Acme Corporation"

    def test_other_rows_untouched(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Changed",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        # Rows 1–4 should be unchanged
        for idx in [1, 2, 3, 4]:
            assert result.clean_df.loc[idx, "vendor_name"] == df.loc[idx, "vendor_name"]

    def test_original_df_not_mutated(self):
        df = _make_df()
        original_val = df.loc[0, "vendor_name"]
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Changed",
        )
        execute_approved_cleanse(issue, df, approved_by="analyst")
        assert df.loc[0, "vendor_name"] == original_val

    def test_invalid_row_indices_silently_skipped(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[999],  # doesn't exist
            canonical_value="Changed",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        # Returns success=True with 0 rows affected (no crash)
        assert result.success is True
        assert result.rows_affected == 0

    def test_currency_noise_coerced_on_numeric_column(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="amount",
            target_row_indices=[0],
            canonical_value="$1,500.00",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert result.clean_df.loc[0, "amount"] == pytest.approx(1500.0)

    def test_audit_before_values_captured(self):
        df = _make_df()
        log = AuditLog()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="New Name",
        )
        execute_approved_cleanse(issue, df, approved_by="analyst", audit_log=log)
        entry = log.entries()[0]
        assert "vendor_name" in entry.before_values
        assert entry.before_values["vendor_name"][0] == "Acme Corp."

    def test_audit_after_values_captured(self):
        df = _make_df()
        log = AuditLog()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="New Name",
        )
        execute_approved_cleanse(issue, df, approved_by="analyst", audit_log=log)
        entry = log.entries()[0]
        assert entry.after_values["vendor_name"][0] == "New Name"

    def test_issue_status_updated_to_executed(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Changed",
        )
        execute_approved_cleanse(issue, df, approved_by="analyst")
        assert issue.status == IssueStatus.EXECUTED
        assert issue.executed_at is not None

    def test_missing_column_raises_cleanly(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="nonexistent_column",
            target_row_indices=[0],
            canonical_value="x",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is False
        assert "nonexistent_column" in result.error


# ---------------------------------------------------------------------------
# CLEAR_VALUE
# ---------------------------------------------------------------------------


class TestClearValue:
    def test_cells_set_to_na(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.CLEAR_VALUE,
            target_column="state",
            target_row_indices=[0, 1],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert pd.isna(result.clean_df.loc[0, "state"])
        assert pd.isna(result.clean_df.loc[1, "state"])

    def test_other_cells_untouched(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.CLEAR_VALUE,
            target_column="state",
            target_row_indices=[0],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        for idx in [1, 2, 3, 4]:
            assert result.clean_df.loc[idx, "state"] == df.loc[idx, "state"]

    def test_audit_entry_written(self):
        df = _make_df()
        log = AuditLog()
        issue = _approved_issue(
            ActionType.CLEAR_VALUE,
            target_column="state",
            target_row_indices=[0],
        )
        execute_approved_cleanse(issue, df, approved_by="analyst", audit_log=log)
        assert len(log) == 1
        assert log.entries()[0].success is True


# ---------------------------------------------------------------------------
# MERGE_ROWS
# ---------------------------------------------------------------------------


class TestMergeRows:
    def test_retired_rows_removed(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[1],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert 1 not in result.clean_df.index

    def test_retain_row_survives(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[1],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert 0 in result.clean_df.index

    def test_scalar_canonical_applied_to_retain(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[1],
            target_column="vendor_name",
            canonical_value="Acme Corporation",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.clean_df.loc[0, "vendor_name"] == "Acme Corporation"

    def test_dict_canonical_applies_per_column(self):
        df = _make_df()
        canonical = {
            "vendor_name": "Acme Corporation",
            "state": "IL",
            "amount": 1500.0,
        }
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[1],
            canonical_value=canonical,
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.clean_df.loc[0, "vendor_name"] == "Acme Corporation"
        assert result.clean_df.loc[0, "state"] == "IL"
        assert result.clean_df.loc[0, "amount"] == pytest.approx(1500.0)

    def test_multiple_retirements(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[1, 2, 3],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert len(result.clean_df) == 2  # 5 - 3 retired = 2

    def test_invalid_retire_indices_skipped(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[999],  # doesn't exist
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert len(result.clean_df) == 5  # nothing dropped

    def test_invalid_retain_index_raises_error(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=999,
            retire_row_indices=[0],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is False
        assert "retain_row_index" in result.error

    def test_audit_entry_records_retained_retired(self):
        df = _make_df()
        log = AuditLog()
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[1],
        )
        execute_approved_cleanse(issue, df, approved_by="analyst", audit_log=log)
        entry = log.entries()[0]
        assert entry.after_values.get("retained_row") == 0
        assert 1 in entry.after_values.get("retired_rows", [])

    def test_row_count_decreases_correctly(self):
        df = _make_df()
        original_len = len(df)
        issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[1, 2],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert len(result.clean_df) == original_len - 2


# ---------------------------------------------------------------------------
# DROP_ROW
# ---------------------------------------------------------------------------


class TestDropRow:
    def test_specified_rows_removed(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.DROP_ROW,
            drop_row_indices=[2, 3],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert 2 not in result.clean_df.index
        assert 3 not in result.clean_df.index

    def test_other_rows_untouched(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.DROP_ROW,
            drop_row_indices=[0],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        for idx in [1, 2, 3, 4]:
            assert idx in result.clean_df.index

    def test_invalid_indices_silently_skipped(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.DROP_ROW,
            drop_row_indices=[999],
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert len(result.clean_df) == 5

    def test_audit_entry_records_dropped_rows(self):
        df = _make_df()
        log = AuditLog()
        issue = _approved_issue(
            ActionType.DROP_ROW,
            drop_row_indices=[0, 1],
        )
        execute_approved_cleanse(issue, df, approved_by="analyst", audit_log=log)
        entry = log.entries()[0]
        assert 0 in entry.after_values.get("dropped_rows", [])
        assert 1 in entry.after_values.get("dropped_rows", [])


# ---------------------------------------------------------------------------
# RETYPE_COLUMN
# ---------------------------------------------------------------------------


class TestRetypeColumn:
    def test_cast_string_to_float(self):
        df = pd.DataFrame({"amount": ["1000", "2000", "3000"]})
        issue = _approved_issue(
            ActionType.RETYPE_COLUMN,
            target_column="amount",
            target_dtype="float64",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert result.clean_df["amount"].dtype in (
            "float64", "Float64"  # numpy or nullable
        ) or str(result.clean_df["amount"].dtype).lower().startswith("float")

    def test_cast_string_to_datetime(self):
        df = pd.DataFrame({"invoice_date": ["2024-01-15", "2024-02-28", "2024-03-01"]})
        issue = _approved_issue(
            ActionType.RETYPE_COLUMN,
            target_column="invoice_date",
            target_dtype="datetime",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert pd.api.types.is_datetime64_any_dtype(result.clean_df["invoice_date"])

    def test_unparseable_values_become_na(self):
        df = pd.DataFrame({"amount": ["100", "not_a_number", "300"]})
        issue = _approved_issue(
            ActionType.RETYPE_COLUMN,
            target_column="amount",
            target_dtype="float64",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is True
        assert pd.isna(result.clean_df.loc[1, "amount"])

    def test_audit_captures_dtype_before_after(self):
        df = pd.DataFrame({"amount": ["100", "200"]})
        log = AuditLog()
        issue = _approved_issue(
            ActionType.RETYPE_COLUMN,
            target_column="amount",
            target_dtype="float64",
        )
        execute_approved_cleanse(issue, df, approved_by="analyst", audit_log=log)
        entry = log.entries()[0]
        assert "dtype_before" in entry.before_values.get("amount", {})
        assert "dtype_after" in entry.after_values.get("amount", {})

    def test_missing_target_column_fails(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.RETYPE_COLUMN,
            target_column="nonexistent",
            target_dtype="float64",
        )
        result = execute_approved_cleanse(issue, df, approved_by="analyst")
        assert result.success is False


# ---------------------------------------------------------------------------
# execute_all_approved
# ---------------------------------------------------------------------------


class TestExecuteAllApproved:
    def _make_issues(self) -> list[Issue]:
        """Return a mixed bag of issues to test filtering and ordering."""
        approved_set = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Acme Corporation",
        )
        approved_merge = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=2,
            retire_row_indices=[3],
        )
        open_issue = _issue_with_status(IssueStatus.OPEN)
        flag_only = _approved_issue(ActionType.FLAG_ONLY)
        rejected = _issue_with_status(IssueStatus.REJECTED)
        return [approved_set, approved_merge, open_issue, flag_only, rejected]

    def test_only_approved_actionable_executed(self):
        df = _make_df()
        issues = self._make_issues()
        result = execute_all_approved(issues, df, approved_by="analyst")
        assert result.succeeded == 2
        assert result.skipped == 3

    def test_flag_only_counted_as_skipped(self):
        df = _make_df()
        flag = _approved_issue(ActionType.FLAG_ONLY)
        result = execute_all_approved([flag], df, approved_by="analyst")
        assert result.skipped == 1
        assert result.succeeded == 0

    def test_open_issues_counted_as_skipped(self):
        df = _make_df()
        issue = _issue_with_status(IssueStatus.OPEN)
        result = execute_all_approved([issue], df, approved_by="analyst")
        assert result.skipped == 1

    def test_set_value_applied_in_result_df(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Acme Corporation",
        )
        result = execute_all_approved([issue], df, approved_by="analyst")
        assert result.clean_df.loc[0, "vendor_name"] == "Acme Corporation"

    def test_merge_applied_after_set_value(self):
        """SET_VALUE on a row that will later be merged: value should be applied
        to the retained row in the final df."""
        df = _make_df()
        set_issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Acme Corporation",
        )
        merge_issue = _approved_issue(
            ActionType.MERGE_ROWS,
            retain_row_index=0,
            retire_row_indices=[1],
        )
        result = execute_all_approved([set_issue, merge_issue], df, approved_by="analyst")
        assert result.succeeded == 2
        assert 1 not in result.clean_df.index
        assert result.clean_df.loc[0, "vendor_name"] == "Acme Corporation"

    def test_retype_before_set_value(self):
        """RETYPE_COLUMN must run before SET_VALUE so the dtype is correct."""
        df = pd.DataFrame({"amount": ["100", "200", "300"]})
        retype = _approved_issue(
            ActionType.RETYPE_COLUMN,
            target_column="amount",
            target_dtype="float64",
        )
        set_val = _approved_issue(
            ActionType.SET_VALUE,
            target_column="amount",
            target_row_indices=[0],
            canonical_value=999.0,
        )
        result = execute_all_approved([set_val, retype], df, approved_by="analyst")
        assert result.succeeded == 2
        assert result.clean_df.loc[0, "amount"] == pytest.approx(999.0)

    def test_total_count_includes_all_issues(self):
        df = _make_df()
        issues = self._make_issues()  # 5 issues
        result = execute_all_approved(issues, df, approved_by="analyst")
        assert result.total == 5

    def test_all_succeeded_true_when_no_failures(self):
        df = _make_df()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Changed",
        )
        result = execute_all_approved([issue], df, approved_by="analyst")
        assert result.all_succeeded is True

    def test_all_succeeded_false_when_failure(self):
        df = _make_df()
        bad_issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="nonexistent",
            target_row_indices=[0],
            canonical_value="x",
        )
        result = execute_all_approved([bad_issue], df, approved_by="analyst")
        assert result.all_succeeded is False

    def test_audit_entries_collected(self):
        df = _make_df()
        log = AuditLog()
        issues = self._make_issues()
        execute_all_approved(issues, df, approved_by="analyst", audit_log=log)
        # 2 approved-actionable issues → 2 audit entries
        assert len(log) == 2

    def test_original_df_not_mutated(self):
        df = _make_df()
        original = df.copy()
        issue = _approved_issue(
            ActionType.SET_VALUE,
            target_column="vendor_name",
            target_row_indices=[0],
            canonical_value="Changed",
        )
        execute_all_approved([issue], df, approved_by="analyst")
        pd.testing.assert_frame_equal(df, original)

    def test_empty_issue_list(self):
        df = _make_df()
        result = execute_all_approved([], df, approved_by="analyst")
        assert result.succeeded == 0
        assert result.total == 0
        assert result.clean_df is not None
        pd.testing.assert_frame_equal(result.clean_df, df)


# ---------------------------------------------------------------------------
# record_rejection
# ---------------------------------------------------------------------------


class TestRecordRejection:
    def test_issue_status_set_to_rejected(self):
        issue = _issue_with_status(IssueStatus.PENDING_APPROVAL)
        record_rejection(issue, rejected_by="manager", note="Not a duplicate.")
        assert issue.status == IssueStatus.REJECTED

    def test_rejected_by_populated(self):
        issue = _issue_with_status(IssueStatus.PENDING_APPROVAL)
        record_rejection(issue, rejected_by="manager")
        assert issue.rejected_by == "manager"

    def test_rejected_at_populated(self):
        issue = _issue_with_status(IssueStatus.PENDING_APPROVAL)
        record_rejection(issue, rejected_by="manager")
        assert issue.rejected_at is not None

    def test_note_stored_on_issue(self):
        issue = _issue_with_status(IssueStatus.PENDING_APPROVAL)
        record_rejection(issue, rejected_by="manager", note="Intentional.")
        assert issue.reviewer_note == "Intentional."

    def test_audit_entry_written(self):
        log = AuditLog()
        issue = _issue_with_status(IssueStatus.PENDING_APPROVAL)
        record_rejection(issue, rejected_by="manager", audit_log=log)
        assert len(log) == 1

    def test_audit_entry_success_false(self):
        log = AuditLog()
        issue = _issue_with_status(IssueStatus.PENDING_APPROVAL)
        record_rejection(issue, rejected_by="manager", audit_log=log)
        assert log.entries()[0].success is False

    def test_audit_entry_notes_contain_rejected(self):
        log = AuditLog()
        issue = _issue_with_status(IssueStatus.PENDING_APPROVAL)
        record_rejection(issue, rejected_by="manager", note="Wrong.", audit_log=log)
        assert "REJECTED" in log.entries()[0].notes
        assert "Wrong." in log.entries()[0].notes


# ---------------------------------------------------------------------------
# AuditLog
# ---------------------------------------------------------------------------


class TestAuditLog:
    def _entry(self, dataset_id="ds-001", issue_id="DQ-001") -> AuditEntry:
        return AuditEntry(
            dataset_id=dataset_id,
            issue_id=issue_id,
            action_type="SET_VALUE",
            actor="analyst",
            success=True,
        )

    def test_append_and_len(self):
        log = AuditLog()
        log.append(self._entry())
        assert len(log) == 1

    def test_entries_returns_copy(self):
        log = AuditLog()
        log.append(self._entry())
        entries = log.entries()
        entries.clear()
        assert len(log) == 1  # original unaffected

    def test_entries_for_dataset_filter(self):
        log = AuditLog()
        log.append(self._entry(dataset_id="ds-001"))
        log.append(self._entry(dataset_id="ds-002"))
        log.append(self._entry(dataset_id="ds-001"))
        result = log.entries_for_dataset("ds-001")
        assert len(result) == 2
        assert all(e.dataset_id == "ds-001" for e in result)

    def test_entries_for_issue_filter(self):
        log = AuditLog()
        log.append(self._entry(issue_id="DQ-001"))
        log.append(self._entry(issue_id="DQ-002"))
        result = log.entries_for_issue("DQ-001")
        assert len(result) == 1
        assert result[0].issue_id == "DQ-001"

    def test_empty_log(self):
        log = AuditLog()
        assert len(log) == 0
        assert log.entries() == []

    def test_multiple_appends_ordered(self):
        log = AuditLog()
        for i in range(5):
            log.append(self._entry(issue_id=f"DQ-{i:03d}"))
        ids = [e.issue_id for e in log.entries()]
        assert ids == [f"DQ-{i:03d}" for i in range(5)]

    def test_audit_entry_has_unique_id(self):
        e1 = self._entry()
        e2 = self._entry()
        assert e1.entry_id != e2.entry_id