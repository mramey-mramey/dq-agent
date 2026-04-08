"""
tests/test_quality_checks.py

Unit tests for backend/tools/quality_checks.py.

Every test drives against either the shared sample_vendor_data.csv fixture
(realistic messy data) or small inline DataFrames crafted to isolate one
specific behaviour.

Coverage:
    - run_quality_checks(): report shape, category filtering, error resilience
    - COMPLETENESS: null detection, blank string detection, severity scaling
    - UNIQUENESS: exact duplicate row grouping, retain/retire indices
    - DEDUPLICATION: high-confidence merge proposal, low-confidence flag-only,
      canonical form selection, no false positives on clearly different strings
    - FORMAT_VALIDITY: currency noise in amount cols, unparseable date cols
    - REFERENTIAL_INTEGRITY: invalid values flagged, valid values clean
    - CONSISTENCY: same key → multiple values flagged, consistent key clean
    - OUTLIER: extreme values flagged, insufficient data skipped
    - DOMAIN_RULE: rule violations flagged, fix_value proposal, compliant rows clean
    - DQReport: finalize() counts, summary_text(), issues_by_category()
    - Issue model: can_bulk_approve(), is_actionable()
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

os.environ.setdefault("DQ_MAX_RECORDS_PER_RUN", "50000")
os.environ.setdefault("OUTPUT_DIR", tempfile.mkdtemp(prefix="dq_test_"))

from backend.models.dataset import ColumnMeta, DatasetMeta, FileFormat, SourceType
from backend.models.issue import (
    ActionType,
    IssueSeverity,
    IssueCategory,
    IssueStatus,
)
from backend.tools.quality_checks import (
    DomainRule,
    run_quality_checks,
    _pick_canonical,
)

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE_CSV = FIXTURES / "sample_vendor_data.csv"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _meta(df: pd.DataFrame, dataset_id: str = "test-dataset-001") -> DatasetMeta:
    """Build a minimal DatasetMeta for a given DataFrame."""
    return DatasetMeta(
        dataset_id=dataset_id,
        source_type=SourceType.FILE,
        file_format=FileFormat.CSV,
        row_count=len(df),
        column_count=len(df.columns),
        columns=[
            ColumnMeta(
                name=str(c),
                dtype=str(df[c].dtype),
                nullable=bool(df[c].isna().any()),
                unique_count=int(df[c].nunique()),
                sample_values=[],
            )
            for c in df.columns
        ],
    )


def _load_fixture() -> tuple[pd.DataFrame, DatasetMeta]:
    df = pd.read_csv(SAMPLE_CSV)
    return df, _meta(df)


# ---------------------------------------------------------------------------
# DQReport shape
# ---------------------------------------------------------------------------


class TestDQReport:
    def test_report_has_dataset_id(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        assert report.dataset_id == meta.dataset_id

    def test_report_has_run_id(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        assert report.run_id is not None

    def test_completed_at_set(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        assert report.completed_at is not None

    def test_total_count_matches_issues_list(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        assert report.total_count == len(report.issues)

    def test_severity_counts_sum_to_total(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        assert (
            report.critical_count + report.high_count
            + report.medium_count + report.low_count
        ) == report.total_count

    def test_categories_checked_populated(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        assert len(report.categories_checked) > 0

    def test_summary_text_contains_dataset_id(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        assert meta.dataset_id in report.summary_text()

    def test_issues_by_category_keys_are_strings(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        for key in report.issues_by_category().keys():
            assert isinstance(key, str)

    def test_category_filter_limits_output(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        for issue in report.issues:
            assert issue.category == IssueCategory.COMPLETENESS

    def test_unknown_category_skipped_gracefully(self):
        df, meta = _load_fixture()
        # Should not raise
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS", "FAKE_CATEGORY"])
        assert report is not None

    def test_all_issues_reference_correct_dataset_id(self):
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta)
        for issue in report.issues:
            assert issue.dataset_id == meta.dataset_id


# ---------------------------------------------------------------------------
# 1. COMPLETENESS
# ---------------------------------------------------------------------------


class TestCompleteness:
    def test_null_field_flagged(self):
        df = pd.DataFrame({"vendor_name": ["Acme", None, "Globex"], "amount": [100, 200, 300]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        completeness_issues = [i for i in report.issues if i.category == IssueCategory.COMPLETENESS]
        assert any("vendor_name" in i.affected_columns for i in completeness_issues)

    def test_blank_string_flagged(self):
        df = pd.DataFrame({"vendor_name": ["Acme", "   ", "Globex"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.COMPLETENESS]
        assert len(issues) > 0

    def test_fully_populated_column_clean(self):
        df = pd.DataFrame({"vendor_name": ["Acme", "Globex", "Initech"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.COMPLETENESS]
        assert len(issues) == 0

    def test_required_columns_subset_respected(self):
        df = pd.DataFrame({"name": ["Acme", None], "code": [None, None]})
        meta = _meta(df)
        # Only check 'name' — 'code' nulls should not produce issues
        report = run_quality_checks(
            df, meta, rule_categories=["COMPLETENESS"], required_columns=["name"]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.COMPLETENESS]
        for issue in issues:
            assert "name" in issue.affected_columns
            assert "code" not in issue.affected_columns

    def test_severity_critical_when_majority_null(self):
        data = {"vendor_name": [None] * 60 + ["Acme"] * 40}
        df = pd.DataFrame(data)
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.COMPLETENESS]
        assert any(i.severity == IssueSeverity.CRITICAL for i in issues)

    def test_severity_low_when_few_nulls(self):
        data = {"vendor_name": [None] + ["Acme"] * 99}
        df = pd.DataFrame(data)
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.COMPLETENESS]
        assert any(i.severity == IssueSeverity.LOW for i in issues)

    def test_fixture_completeness_issues_found(self):
        """Sample fixture has known nulls in address, zip, tax_id, ytd_spend."""
        df, meta = _load_fixture()
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.COMPLETENESS]
        assert len(issues) > 0

    def test_affected_row_indices_are_ints(self):
        df = pd.DataFrame({"col": [None, "val", None]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        for issue in report.issues:
            for idx in issue.affected_row_indices:
                assert isinstance(idx, int)

    def test_proposed_action_is_flag_only(self):
        df = pd.DataFrame({"vendor_name": [None, "Acme"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        for issue in report.issues:
            assert issue.proposed_action.action_type == ActionType.FLAG_ONLY


# ---------------------------------------------------------------------------
# 2. UNIQUENESS
# ---------------------------------------------------------------------------


class TestUniqueness:
    def _dup_df(self):
        return pd.DataFrame({
            "id": ["V-001", "V-002", "V-001"],
            "name": ["Acme", "Globex", "Acme"],
            "amount": [100.0, 200.0, 100.0],
        })

    def test_exact_duplicates_flagged(self):
        df = self._dup_df()
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["UNIQUENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.UNIQUENESS]
        assert len(issues) > 0

    def test_retain_and_retire_indices_present(self):
        df = self._dup_df()
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["UNIQUENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.UNIQUENESS]
        for issue in issues:
            pa = issue.proposed_action
            assert pa.retain_row_index is not None
            assert len(pa.retire_row_indices) >= 1

    def test_no_unique_data_clean(self):
        df = pd.DataFrame({"id": ["V-001", "V-002", "V-003"], "name": ["A", "B", "C"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["UNIQUENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.UNIQUENESS]
        assert len(issues) == 0

    def test_severity_is_high(self):
        df = self._dup_df()
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["UNIQUENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.UNIQUENESS]
        for issue in issues:
            assert issue.severity == IssueSeverity.HIGH

    def test_action_is_merge_rows(self):
        df = self._dup_df()
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["UNIQUENESS"])
        issues = [i for i in report.issues if i.category == IssueCategory.UNIQUENESS]
        for issue in issues:
            assert issue.proposed_action.action_type == ActionType.MERGE_ROWS


# ---------------------------------------------------------------------------
# 3. DEDUPLICATION
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_high_similarity_flagged(self):
        pytest.importorskip("rapidfuzz")
        df = pd.DataFrame({
            "vendor_name": ["Acme Corp.", "ACME Corporation", "Globex Supplies"],
        })
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DEDUPLICATION"], fuzzy_columns=["vendor_name"]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DEDUPLICATION]
        assert len(issues) >= 1

    def test_high_confidence_gets_merge_proposal(self):
        pytest.importorskip("rapidfuzz")
        df = pd.DataFrame({"vendor_name": ["Acme Corp.", "Acme Corp"]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DEDUPLICATION"], fuzzy_columns=["vendor_name"]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DEDUPLICATION]
        high_conf = [i for i in issues if i.confidence >= 0.92]
        for issue in high_conf:
            assert issue.proposed_action.action_type == ActionType.MERGE_ROWS

    def test_low_confidence_is_flag_only(self):
        pytest.importorskip("rapidfuzz")
        # "Globex Inc" vs "Globex Ltd" — check that issues below FUZZY_HIGH_CONFIDENCE get FLAG_ONLY
        # We test the property: any issue with confidence < 0.88 must be FLAG_ONLY
        df = pd.DataFrame({"vendor_name": ["Acme Corp.", "ACME Corporation", "Totally Different Name"]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DEDUPLICATION"], fuzzy_columns=["vendor_name"]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DEDUPLICATION]
        # Any issue below the high-confidence threshold must be FLAG_ONLY
        for issue in issues:
            if issue.confidence < 0.88:
                assert issue.proposed_action.action_type == ActionType.FLAG_ONLY

    def test_clearly_different_strings_not_flagged(self):
        pytest.importorskip("rapidfuzz")
        df = pd.DataFrame({
            "vendor_name": ["Apple Inc", "Microsoft Corporation", "Amazon Web Services"]
        })
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DEDUPLICATION"], fuzzy_columns=["vendor_name"]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DEDUPLICATION]
        assert len(issues) == 0

    def test_dedup_never_bulk_approvable(self):
        pytest.importorskip("rapidfuzz")
        df = pd.DataFrame({"vendor_name": ["Acme Corp.", "ACME Corporation"]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DEDUPLICATION"], fuzzy_columns=["vendor_name"]
        )
        for issue in report.issues:
            if issue.category == IssueCategory.DEDUPLICATION:
                assert issue.can_bulk_approve(bulk_threshold=0.95) is False

    def test_fixture_vendor_dedups_found(self):
        """Fixture has known fuzzy pairs: Acme Corp./ACME Corporation, etc."""
        pytest.importorskip("rapidfuzz")
        df, meta = _load_fixture()
        report = run_quality_checks(
            df, meta, rule_categories=["DEDUPLICATION"], fuzzy_columns=["vendor_name"]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DEDUPLICATION]
        assert len(issues) >= 4  # Acme, Globex/Initech, Wayne, Reynolds pairs

    def test_canonical_value_set_on_merge_proposal(self):
        pytest.importorskip("rapidfuzz")
        df = pd.DataFrame({"vendor_name": ["Acme Corp.", "Acme Corp"]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DEDUPLICATION"], fuzzy_columns=["vendor_name"]
        )
        for issue in report.issues:
            if issue.proposed_action.action_type == ActionType.MERGE_ROWS:
                assert issue.proposed_action.canonical_value is not None


# ---------------------------------------------------------------------------
# 4. FORMAT VALIDITY
# ---------------------------------------------------------------------------


class TestFormatValidity:
    def test_currency_symbol_in_amount_col_flagged(self):
        df = pd.DataFrame({"ytd_spend": ["$1,500.00", "2000.00", "$750.50"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["FORMAT_VALIDITY"])
        issues = [i for i in report.issues if i.category == IssueCategory.FORMAT_VALIDITY]
        assert len(issues) > 0
        assert any("ytd_spend" in i.affected_columns for i in issues)

    def test_clean_numeric_amount_not_flagged(self):
        df = pd.DataFrame({"ytd_spend": [1500.0, 2000.0, 750.5]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["FORMAT_VALIDITY"])
        issues = [i for i in report.issues if i.category == IssueCategory.FORMAT_VALIDITY]
        assert len(issues) == 0

    def test_unparseable_date_flagged(self):
        df = pd.DataFrame({"invoice_date": ["2024-01-15", "not-a-date", "Jan 5th 2024 approx"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["FORMAT_VALIDITY"])
        issues = [i for i in report.issues if i.category == IssueCategory.FORMAT_VALIDITY]
        date_issues = [i for i in issues if "invoice_date" in i.affected_columns]
        assert len(date_issues) > 0

    def test_valid_dates_not_flagged(self):
        df = pd.DataFrame({"invoice_date": ["2024-01-15", "2024-02-28", "2023-12-01"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["FORMAT_VALIDITY"])
        issues = [i for i in report.issues if i.category == IssueCategory.FORMAT_VALIDITY]
        date_issues = [i for i in issues if "invoice_date" in i.affected_columns]
        assert len(date_issues) == 0

    def test_non_amount_non_date_column_skipped(self):
        df = pd.DataFrame({"vendor_id": ["V-001", "V-002", "abc"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["FORMAT_VALIDITY"])
        issues = [i for i in report.issues if i.category == IssueCategory.FORMAT_VALIDITY]
        assert len(issues) == 0


# ---------------------------------------------------------------------------
# 5. REFERENTIAL INTEGRITY
# ---------------------------------------------------------------------------


class TestReferentialIntegrity:
    def test_invalid_value_flagged(self):
        df = pd.DataFrame({"payment_terms": ["NET30", "NET45", "UNKNOWN_TERM"]})
        meta = _meta(df)
        valid = {"NET30", "NET45", "NET60", "NET90"}
        report = run_quality_checks(
            df, meta,
            rule_categories=["REFERENTIAL_INTEGRITY"],
            reference_sets={"payment_terms": valid},
        )
        issues = [i for i in report.issues if i.category == IssueCategory.REFERENTIAL_INTEGRITY]
        assert len(issues) == 1
        assert 2 in issues[0].affected_row_indices

    def test_all_valid_values_clean(self):
        df = pd.DataFrame({"payment_terms": ["NET30", "NET45", "NET60"]})
        meta = _meta(df)
        valid = {"NET30", "NET45", "NET60", "NET90"}
        report = run_quality_checks(
            df, meta,
            rule_categories=["REFERENTIAL_INTEGRITY"],
            reference_sets={"payment_terms": valid},
        )
        issues = [i for i in report.issues if i.category == IssueCategory.REFERENTIAL_INTEGRITY]
        assert len(issues) == 0

    def test_missing_column_skipped_gracefully(self):
        df = pd.DataFrame({"other_col": ["a", "b"]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta,
            rule_categories=["REFERENTIAL_INTEGRITY"],
            reference_sets={"nonexistent_col": {"x"}},
        )
        # Should not raise; issues list may be empty
        assert report is not None

    def test_ri_skipped_without_reference_sets(self):
        df, meta = _load_fixture()
        # Don't pass reference_sets and don't include RI in categories
        report = run_quality_checks(df, meta, rule_categories=["COMPLETENESS"])
        ri_issues = [i for i in report.issues if i.category == IssueCategory.REFERENTIAL_INTEGRITY]
        assert len(ri_issues) == 0

    def test_fixture_payment_terms_ri(self):
        df, meta = _load_fixture()
        valid_terms = {"NET30", "NET45", "NET60", "NET90"}
        report = run_quality_checks(
            df, meta,
            rule_categories=["REFERENTIAL_INTEGRITY"],
            reference_sets={"payment_terms": valid_terms},
        )
        # Fixture has blank payment_terms for V-1004, V-1007, V-1013 — nulls are skipped by RI
        # All non-null values should be in valid_terms
        issues = [i for i in report.issues if i.category == IssueCategory.REFERENTIAL_INTEGRITY]
        assert len(issues) == 0


# ---------------------------------------------------------------------------
# 6. CONSISTENCY
# ---------------------------------------------------------------------------


class TestConsistency:
    def test_inconsistent_key_flagged(self):
        # Same tax_id maps to two different vendor_names
        df = pd.DataFrame({
            "tax_id": ["47-1234567", "47-1234567", "82-9876543"],
            "vendor_name": ["Acme Corp.", "ACME Corporation", "Globex Supplies"],
        })
        meta = _meta(df)
        report = run_quality_checks(
            df, meta,
            rule_categories=["CONSISTENCY"],
            consistency_checks=[(["tax_id"], "vendor_name")],
        )
        issues = [i for i in report.issues if i.category == IssueCategory.CONSISTENCY]
        assert len(issues) == 1
        assert "vendor_name" in issues[0].affected_columns

    def test_consistent_key_clean(self):
        df = pd.DataFrame({
            "tax_id": ["47-1234567", "47-1234567", "82-9876543"],
            "vendor_name": ["Acme Corp.", "Acme Corp.", "Globex Supplies"],
        })
        meta = _meta(df)
        report = run_quality_checks(
            df, meta,
            rule_categories=["CONSISTENCY"],
            consistency_checks=[(["tax_id"], "vendor_name")],
        )
        issues = [i for i in report.issues if i.category == IssueCategory.CONSISTENCY]
        assert len(issues) == 0

    def test_multiple_consistency_rules(self):
        df = pd.DataFrame({
            "tax_id": ["AAA", "AAA", "BBB", "BBB"],
            "vendor_name": ["Alpha", "Alpha Corp", "Beta", "Beta"],
            "state": ["IL", "IL", "OH", "IN"],  # BBB has two states
        })
        meta = _meta(df)
        report = run_quality_checks(
            df, meta,
            rule_categories=["CONSISTENCY"],
            consistency_checks=[
                (["tax_id"], "vendor_name"),
                (["tax_id"], "state"),
            ],
        )
        issues = [i for i in report.issues if i.category == IssueCategory.CONSISTENCY]
        assert len(issues) == 2

    def test_fixture_tax_id_consistency(self):
        """Fixture has known pairs sharing tax_id with different vendor_names."""
        df, meta = _load_fixture()
        report = run_quality_checks(
            df, meta,
            rule_categories=["CONSISTENCY"],
            consistency_checks=[(["tax_id"], "vendor_name")],
        )
        issues = [i for i in report.issues if i.category == IssueCategory.CONSISTENCY]
        # Acme, Globex, Initech, Wayne, Reynolds all share a tax_id across two names
        assert len(issues) >= 4


# ---------------------------------------------------------------------------
# 7. OUTLIERS
# ---------------------------------------------------------------------------


class TestOutliers:
    def test_extreme_value_flagged(self):
        # 99 normal values + 1 extreme outlier
        values = [100.0] * 99 + [999999.0]
        df = pd.DataFrame({"ytd_spend": values})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["OUTLIER"])
        issues = [i for i in report.issues if i.category == IssueCategory.OUTLIER]
        assert len(issues) == 1
        assert 99 in issues[0].affected_row_indices

    def test_no_outliers_in_uniform_data(self):
        df = pd.DataFrame({"amount": [100.0, 100.0, 100.0, 100.0, 100.0] * 5})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["OUTLIER"])
        issues = [i for i in report.issues if i.category == IssueCategory.OUTLIER]
        assert len(issues) == 0

    def test_insufficient_data_skipped(self):
        # Only 5 rows — below OUTLIER_MIN_VALUES (10)
        df = pd.DataFrame({"amount": [100.0, 200.0, 150.0, 300.0, 50000.0]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["OUTLIER"])
        issues = [i for i in report.issues if i.category == IssueCategory.OUTLIER]
        assert len(issues) == 0

    def test_proposed_action_is_flag_only(self):
        values = [100.0] * 50 + [999999.0]
        df = pd.DataFrame({"amount": values})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["OUTLIER"])
        for issue in report.issues:
            if issue.category == IssueCategory.OUTLIER:
                assert issue.proposed_action.action_type == ActionType.FLAG_ONLY

    def test_non_numeric_columns_skipped(self):
        df = pd.DataFrame({"vendor_name": ["Acme"] * 50 + ["OUTLIER_TEXT"]})
        meta = _meta(df)
        report = run_quality_checks(df, meta, rule_categories=["OUTLIER"])
        issues = [i for i in report.issues if i.category == IssueCategory.OUTLIER]
        assert len(issues) == 0


# ---------------------------------------------------------------------------
# 8. DOMAIN RULES
# ---------------------------------------------------------------------------


class TestDomainRules:
    def _negative_rule(self) -> DomainRule:
        return DomainRule(
            column="ytd_spend",
            rule_fn=lambda v: v >= 0,
            description="YTD spend must not be negative.",
            severity=IssueSeverity.HIGH,
        )

    def test_rule_violation_flagged(self):
        df = pd.DataFrame({"ytd_spend": [100.0, -50.0, 200.0]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DOMAIN_RULE"], domain_rules=[self._negative_rule()]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DOMAIN_RULE]
        assert len(issues) == 1
        assert 1 in issues[0].affected_row_indices

    def test_compliant_rows_not_flagged(self):
        df = pd.DataFrame({"ytd_spend": [100.0, 50.0, 200.0]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DOMAIN_RULE"], domain_rules=[self._negative_rule()]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DOMAIN_RULE]
        assert len(issues) == 0

    def test_fix_value_produces_set_value_action(self):
        rule = DomainRule(
            column="qty",
            rule_fn=lambda v: v >= 0,
            description="Quantity cannot be negative.",
            severity=IssueSeverity.MEDIUM,
            fix_value=0,
        )
        df = pd.DataFrame({"qty": [5, -3, 10]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DOMAIN_RULE"], domain_rules=[rule]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DOMAIN_RULE]
        assert issues[0].proposed_action.action_type == ActionType.SET_VALUE
        assert issues[0].proposed_action.canonical_value == 0

    def test_no_fix_value_produces_flag_only(self):
        df = pd.DataFrame({"ytd_spend": [-100.0]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DOMAIN_RULE"], domain_rules=[self._negative_rule()]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DOMAIN_RULE]
        assert issues[0].proposed_action.action_type == ActionType.FLAG_ONLY

    def test_missing_column_skipped_gracefully(self):
        df = pd.DataFrame({"other": [1, 2, 3]})
        meta = _meta(df)
        rule = DomainRule("nonexistent_col", lambda v: True, "desc")
        report = run_quality_checks(
            df, meta, rule_categories=["DOMAIN_RULE"], domain_rules=[rule]
        )
        assert report is not None

    def test_multiple_domain_rules(self):
        rule1 = DomainRule("amount", lambda v: v > 0, "Amount must be positive.", IssueSeverity.HIGH)
        rule2 = DomainRule("qty", lambda v: v >= 0, "Quantity must be non-negative.", IssueSeverity.MEDIUM)
        df = pd.DataFrame({"amount": [-100.0, 200.0], "qty": [5, -1]})
        meta = _meta(df)
        report = run_quality_checks(
            df, meta, rule_categories=["DOMAIN_RULE"], domain_rules=[rule1, rule2]
        )
        issues = [i for i in report.issues if i.category == IssueCategory.DOMAIN_RULE]
        assert len(issues) == 2


# ---------------------------------------------------------------------------
# Issue model
# ---------------------------------------------------------------------------


class TestIssueModel:
    def test_is_actionable_true_for_set_value(self):
        from backend.models.issue import Issue, ProposedAction
        issue = Issue(
            dataset_id="x",
            category=IssueCategory.COMPLETENESS,
            severity=IssueSeverity.LOW,
            description="test",
            proposed_action=ProposedAction(action_type=ActionType.SET_VALUE, target_column="col"),
        )
        assert issue.is_actionable() is True

    def test_is_actionable_false_for_flag_only(self):
        from backend.models.issue import Issue, ProposedAction
        issue = Issue(
            dataset_id="x",
            category=IssueCategory.COMPLETENESS,
            severity=IssueSeverity.LOW,
            description="test",
            proposed_action=ProposedAction(action_type=ActionType.FLAG_ONLY),
        )
        assert issue.is_actionable() is False

    def test_bulk_approve_blocked_for_dedup(self):
        from backend.models.issue import Issue
        issue = Issue(
            dataset_id="x",
            category=IssueCategory.DEDUPLICATION,
            severity=IssueSeverity.HIGH,
            description="test",
            confidence=0.99,
        )
        assert issue.can_bulk_approve(bulk_threshold=0.95) is False

    def test_bulk_approve_allowed_for_high_confidence_non_dedup(self):
        from backend.models.issue import Issue
        issue = Issue(
            dataset_id="x",
            category=IssueCategory.FORMAT_VALIDITY,
            severity=IssueSeverity.MEDIUM,
            description="test",
            confidence=0.97,
        )
        assert issue.can_bulk_approve(bulk_threshold=0.95) is True

    def test_bulk_approve_blocked_below_threshold(self):
        from backend.models.issue import Issue
        issue = Issue(
            dataset_id="x",
            category=IssueCategory.FORMAT_VALIDITY,
            severity=IssueSeverity.MEDIUM,
            description="test",
            confidence=0.80,
        )
        assert issue.can_bulk_approve(bulk_threshold=0.95) is False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_pick_canonical_prefers_longer(self):
        assert _pick_canonical("Acme Corp.", "Acme") == "Acme Corp."
        assert _pick_canonical("a", "longer string") == "Longer String"

    def test_pick_canonical_title_cases(self):
        result = _pick_canonical("ACME CORPORATION", "acme corp")
        assert result == result.title()
