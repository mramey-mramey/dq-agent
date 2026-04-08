"""
backend/tools/quality_checks.py

Quality Check Engine for the DQ agent.

Entry point:
    run_quality_checks(df, meta, rule_categories=None) -> DQReport

Seven rule categories (all enabled by default):
    1. COMPLETENESS          — null / blank required fields
    2. UNIQUENESS            — exact duplicate rows
    3. DEDUPLICATION         — fuzzy entity matching (vendor name, etc.)
    4. FORMAT_VALIDITY       — malformed dates, mixed-currency amounts, etc.
    5. REFERENTIAL_INTEGRITY — column values not in a provided reference set
    6. CONSISTENCY           — conflicting values for the same entity key
    7. OUTLIER               — values > N standard deviations from column mean
    8. DOMAIN_RULE           — configurable business rules (negative, future date, etc.)

Design constraints (from CLAUDE.md):
    - This module is READ-ONLY. It never modifies the DataFrame.
    - Each check function returns a list[Issue]; the engine aggregates them.
    - Confidence below DQ_CONFIDENCE_THRESHOLD → FLAG_ONLY action, no auto-proposal.
    - Deduplication confidence is always surfaced explicitly so the UI can
      prevent bulk-approval of merge proposals.
    - All Issue descriptions are written in plain English for non-technical analysts.

Dependencies:
    pip install pandas rapidfuzz python-dateutil
"""

from __future__ import annotations

import logging
import os
import re
import statistics
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from backend.models.dataset import DatasetMeta
from backend.models.issue import (
    ActionType,
    DQReport,
    Issue,
    IssueCategory,
    IssueSeverity,
    IssueStatus,
    ProposedAction,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIDENCE_THRESHOLD = float(os.getenv("DQ_CONFIDENCE_THRESHOLD", "0.80"))

# Fuzzy match: similarity score (0–100) above which two strings are "likely same entity"
# Scoring uses max(token_set_ratio, partial_ratio) on lowercased strings, which handles:
#   - case differences ("ACME" vs "Acme")
#   - abbreviations ("Corp." vs "Corporation")
#   - suffix differences ("Ltd." vs "LLC")
FUZZY_HIGH_CONFIDENCE = 88      # → DEDUPLICATION issue with a concrete merge proposal
FUZZY_LOW_CONFIDENCE = 75       # → DEDUPLICATION issue flagged for human review only

# Outlier: number of standard deviations from the mean to flag
OUTLIER_SIGMA = float(os.getenv("DQ_OUTLIER_SIGMA", "3.0"))

# Minimum non-null values in a column before running outlier detection
OUTLIER_MIN_VALUES = 10

# Minimum rows in a string column before running fuzzy dedup
FUZZY_MIN_ROWS = 2

ALL_CATEGORIES = [
    IssueCategory.COMPLETENESS,
    IssueCategory.UNIQUENESS,
    IssueCategory.DEDUPLICATION,
    IssueCategory.FORMAT_VALIDITY,
    IssueCategory.CONSISTENCY,
    IssueCategory.OUTLIER,
    IssueCategory.DOMAIN_RULE,
    # REFERENTIAL_INTEGRITY is skipped by default — requires caller-supplied ref sets
]

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_quality_checks(
    df: pd.DataFrame,
    meta: DatasetMeta,
    rule_categories: list[str] | None = None,
    *,
    # --- Optional configuration for specific checks ---
    # COMPLETENESS: column names that must be non-null
    required_columns: list[str] | None = None,
    # REFERENTIAL_INTEGRITY: {column_name: set_of_valid_values}
    reference_sets: dict[str, set] | None = None,
    # CONSISTENCY: list of (key_columns, value_column) pairs to check
    consistency_checks: list[tuple[list[str], str]] | None = None,
    # DOMAIN_RULE: list of DomainRule instances
    domain_rules: list[DomainRule] | None = None,
    # DEDUPLICATION: column names to run fuzzy matching on
    fuzzy_columns: list[str] | None = None,
) -> DQReport:
    """
    Run all configured DQ checks against a DataFrame.

    Args:
        df:                 The ingested DataFrame (read-only — never modified).
        meta:               DatasetMeta from the ingest step.
        rule_categories:    Subset of IssueCategory values to run.
                            Defaults to ALL_CATEGORIES (minus REFERENTIAL_INTEGRITY
                            unless reference_sets is provided).
        required_columns:   COMPLETENESS — columns that must be fully populated.
                            Defaults to all columns.
        reference_sets:     REFERENTIAL_INTEGRITY — {col: {valid_val, ...}}.
                            If None, referential integrity is skipped.
        consistency_checks: CONSISTENCY — [(key_cols, value_col), ...].
                            Checks that value_col is consistent across rows
                            sharing the same key_cols values.
        domain_rules:       DOMAIN_RULE — list of DomainRule objects.
        fuzzy_columns:      DEDUPLICATION — string columns to fuzzy-match.
                            If None, all object-dtype columns are candidates.

    Returns:
        DQReport with all issues found, summary counts, and timing.
    """
    # Resolve which categories to run
    requested = _resolve_categories(rule_categories, reference_sets)

    report = DQReport(dataset_id=meta.dataset_id, categories_checked=[c.value for c in requested])

    for category in requested:
        try:
            issues = _run_category(
                category=category,
                df=df,
                meta=meta,
                required_columns=required_columns,
                reference_sets=reference_sets or {},
                consistency_checks=consistency_checks or [],
                domain_rules=domain_rules or [],
                fuzzy_columns=fuzzy_columns,
            )
            report.issues.extend(issues)
        except Exception as exc:
            logger.exception("Error running %s checks: %s", category.value, exc)
            # Surface as a low-severity system issue rather than crashing the run
            report.issues.append(
                _system_error_issue(meta.dataset_id, category, str(exc))
            )

    report.finalize()
    logger.info(report.summary_text())
    return report


# ---------------------------------------------------------------------------
# DomainRule — caller-supplied business rules
# ---------------------------------------------------------------------------


class DomainRule:
    """
    A single configurable business rule applied to one column.

    Example:
        DomainRule(
            column="ytd_spend",
            rule_fn=lambda v: v >= 0,
            description="YTD spend must not be negative.",
            severity=IssueSeverity.HIGH,
        )
    """

    def __init__(
        self,
        column: str,
        rule_fn,           # Callable[[Any], bool] — returns True if value is VALID
        description: str,
        severity: IssueSeverity = IssueSeverity.MEDIUM,
        fix_value: Any = None,  # If provided, SET_VALUE to this on failure
    ):
        self.column = column
        self.rule_fn = rule_fn
        self.description = description
        self.severity = severity
        self.fix_value = fix_value


# ---------------------------------------------------------------------------
# Category dispatcher
# ---------------------------------------------------------------------------


def _resolve_categories(
    rule_categories: list[str] | None,
    reference_sets: dict | None,
) -> list[IssueCategory]:
    if rule_categories is None:
        cats = list(ALL_CATEGORIES)
        if reference_sets:
            cats.append(IssueCategory.REFERENTIAL_INTEGRITY)
        return cats

    resolved = []
    for name in rule_categories:
        try:
            resolved.append(IssueCategory(name.upper()))
        except ValueError:
            logger.warning("Unknown rule category '%s' — skipping.", name)
    return resolved


def _run_category(
    category: IssueCategory,
    df: pd.DataFrame,
    meta: DatasetMeta,
    **kwargs,
) -> list[Issue]:
    dispatch = {
        IssueCategory.COMPLETENESS: _check_completeness,
        IssueCategory.UNIQUENESS: _check_uniqueness,
        IssueCategory.DEDUPLICATION: _check_deduplication,
        IssueCategory.FORMAT_VALIDITY: _check_format_validity,
        IssueCategory.REFERENTIAL_INTEGRITY: _check_referential_integrity,
        IssueCategory.CONSISTENCY: _check_consistency,
        IssueCategory.OUTLIER: _check_outliers,
        IssueCategory.DOMAIN_RULE: _check_domain_rules,
    }
    fn = dispatch.get(category)
    if fn is None:
        return []
    return fn(df=df, meta=meta, **kwargs)


# ---------------------------------------------------------------------------
# 1. COMPLETENESS
# ---------------------------------------------------------------------------


def _check_completeness(
    df: pd.DataFrame,
    meta: DatasetMeta,
    required_columns: list[str] | None,
    **_,
) -> list[Issue]:
    """Flag rows where required columns are null or blank."""
    issues: list[Issue] = []
    cols_to_check = required_columns if required_columns is not None else list(df.columns)

    for col in cols_to_check:
        if col not in df.columns:
            logger.warning("COMPLETENESS: column '%s' not in DataFrame — skipping.", col)
            continue

        series = df[col]
        # Null check
        null_mask = series.isna()
        # Blank string check (any string-like column, including pandas StringDtype)
        if pd.api.types.is_string_dtype(series) or series.dtype == object:
            blank_mask = series.astype(str).str.strip() == ""
            # Don't double-count nulls that astype(str) turns into "nan" / "<NA>"
            blank_mask = blank_mask & ~null_mask
            problem_mask = null_mask | blank_mask
        else:
            problem_mask = null_mask

        row_indices = list(df.index[problem_mask])
        if not row_indices:
            continue

        pct = len(row_indices) / len(df) * 100
        severity = (
            IssueSeverity.CRITICAL if pct > 50
            else IssueSeverity.HIGH if pct > 20
            else IssueSeverity.MEDIUM if pct > 5
            else IssueSeverity.LOW
        )

        issues.append(
            Issue(
                dataset_id=meta.dataset_id,
                category=IssueCategory.COMPLETENESS,
                severity=severity,
                affected_row_indices=row_indices,
                affected_columns=[col],
                description=(
                    f"Column '{col}' is missing or blank in {len(row_indices)} row(s) "
                    f"({pct:.1f}% of the dataset). "
                    f"This field should be populated for all records."
                ),
                confidence=1.0,
                raw_values={col: _safe_values(df, col, row_indices)},
                proposed_action=ProposedAction(
                    action_type=ActionType.FLAG_ONLY,
                    target_column=col,
                    target_row_indices=row_indices,
                    rationale=(
                        "Missing values cannot be automatically filled — "
                        "the correct value must be supplied by the analyst."
                    ),
                ),
            )
        )

    return issues


# ---------------------------------------------------------------------------
# 2. UNIQUENESS — exact duplicate rows
# ---------------------------------------------------------------------------


def _check_uniqueness(
    df: pd.DataFrame,
    meta: DatasetMeta,
    **_,
) -> list[Issue]:
    """Flag groups of rows that are exact duplicates across all columns."""
    issues: list[Issue] = []

    dup_mask = df.duplicated(keep=False)
    if not dup_mask.any():
        return issues

    # Group duplicate rows together
    dup_df = df[dup_mask].copy()
    dup_df["_orig_index"] = dup_df.index

    grouped = dup_df.groupby(list(df.columns), dropna=False)["_orig_index"].apply(list)

    for _, row_indices in grouped.items():
        if len(row_indices) < 2:
            continue

        retain = row_indices[0]
        retire = row_indices[1:]

        # Sample the duplicate values for display
        sample_col = df.columns[0]
        raw = {sample_col: _safe_values(df, sample_col, row_indices)}

        issues.append(
            Issue(
                dataset_id=meta.dataset_id,
                category=IssueCategory.UNIQUENESS,
                severity=IssueSeverity.HIGH,
                affected_row_indices=row_indices,
                affected_columns=list(df.columns),
                description=(
                    f"{len(row_indices)} rows are exact duplicates of each other "
                    f"(row indices: {row_indices}). "
                    f"Recommend retaining row {retain} and removing the others."
                ),
                confidence=1.0,
                raw_values=raw,
                proposed_action=ProposedAction(
                    action_type=ActionType.MERGE_ROWS,
                    retain_row_index=retain,
                    retire_row_indices=retire,
                    rationale=(
                        f"All {len(row_indices)} rows are byte-for-byte identical. "
                        f"Row {retain} is retained; rows {retire} are removed."
                    ),
                ),
            )
        )

    return issues


# ---------------------------------------------------------------------------
# 3. DEDUPLICATION — fuzzy entity matching
# ---------------------------------------------------------------------------


def _check_deduplication(
    df: pd.DataFrame,
    meta: DatasetMeta,
    fuzzy_columns: list[str] | None,
    **_,
) -> list[Issue]:
    """
    Fuzzy-match string columns to find records that likely represent the same
    real-world entity despite different spellings.

    Uses rapidfuzz for fast Levenshtein + token-sort scoring.
    """
    try:
        from rapidfuzz import fuzz, process as rfprocess
    except ImportError:
        logger.warning(
            "rapidfuzz not installed — DEDUPLICATION check skipped. "
            "Run: pip install rapidfuzz"
        )
        return []

    issues: list[Issue] = []

    # Determine which columns to check
    if fuzzy_columns is not None:
        candidates = [c for c in fuzzy_columns if c in df.columns]
    else:
        # Default: all string-like columns that look like name/label fields
        candidates = [
            c for c in df.columns
            if (pd.api.types.is_string_dtype(df[c]) or df[c].dtype == object)
            and df[c].nunique() >= FUZZY_MIN_ROWS
        ]

    for col in candidates:
        series = df[col].dropna().astype(str)
        if len(series) < FUZZY_MIN_ROWS:
            continue

        unique_values = series.unique().tolist()
        # Normalized lookup: maps lowercased form back to original value
        norm_map: dict[str, str] = {v.lower().strip(): v for v in unique_values}
        norm_values = list(norm_map.keys())
        already_paired: set[frozenset] = set()

        for val_a_orig in unique_values:
            val_a_norm = val_a_orig.lower().strip()
            # Composite scorer: max(token_set_ratio, partial_ratio) on normalized strings.
            # token_set_ratio handles word-order / subset differences (e.g. "LLC" suffix).
            # partial_ratio catches abbreviations ("Corp." inside "Corporation").
            # Running both and taking the max gives the most robust coverage.
            def _composite(s1: str, s2: str, **kw) -> float:
                return max(fuzz.token_set_ratio(s1, s2), fuzz.partial_ratio(s1, s2))

            matches = rfprocess.extract(
                val_a_norm,
                norm_values,
                scorer=_composite,
                score_cutoff=FUZZY_LOW_CONFIDENCE,
                limit=None,
            )

            for val_b_norm, score, _ in matches:
                val_b_orig = norm_map[val_b_norm]
                if val_a_orig == val_b_orig:
                    continue
                pair = frozenset([val_a_orig, val_b_orig])
                if pair in already_paired:
                    continue
                already_paired.add(pair)

                # Find all row indices for each value
                rows_a = list(df.index[df[col].astype(str) == val_a_orig])
                rows_b = list(df.index[df[col].astype(str) == val_b_orig])
                all_rows = rows_a + rows_b

                confidence = round(score / 100.0, 4)
                canonical = _pick_canonical(val_a_orig, val_b_orig)

                if score >= FUZZY_HIGH_CONFIDENCE:
                    severity = IssueSeverity.HIGH
                    action_type = ActionType.MERGE_ROWS
                    rationale = (
                        f"Similarity score {score:.0f}/100 strongly suggests these are "
                        f"the same entity. Proposed canonical form: '{canonical}'."
                    )
                    retain = rows_a[0]
                    retire = rows_b
                else:
                    severity = IssueSeverity.MEDIUM
                    action_type = ActionType.FLAG_ONLY
                    rationale = (
                        f"Similarity score {score:.0f}/100 is above the detection "
                        f"threshold but below the auto-proposal threshold ({FUZZY_HIGH_CONFIDENCE}). "
                        f"Human review required."
                    )
                    retain = None
                    retire = []

                issues.append(
                    Issue(
                        dataset_id=meta.dataset_id,
                        category=IssueCategory.DEDUPLICATION,
                        severity=severity,
                        affected_row_indices=all_rows,
                        affected_columns=[col],
                        description=(
                            f"Column '{col}': '{val_a_orig}' and '{val_b_orig}' appear to represent "
                            f"the same entity (similarity: {score:.0f}/100). "
                            f"Suggested canonical value: '{canonical}'."
                        ),
                        confidence=confidence,
                        raw_values={col: [val_a_orig, val_b_orig]},
                        proposed_action=ProposedAction(
                            action_type=action_type,
                            target_column=col,
                            target_row_indices=all_rows,
                            canonical_value=canonical if action_type == ActionType.MERGE_ROWS else None,
                            retain_row_index=retain,
                            retire_row_indices=retire,
                            rationale=rationale,
                        ),
                    )
                )

    return issues


def _pick_canonical(a: str, b: str) -> str:
    """
    Heuristic to choose the canonical form of two fuzzy-matched strings.
    Prefers: longer string, then title case, then alphabetically first.
    """
    # Prefer the longer one (more complete)
    if len(a) != len(b):
        longer = a if len(a) > len(b) else b
    else:
        longer = a

    # Apply title case normalisation
    return longer.strip().title()


# ---------------------------------------------------------------------------
# 4. FORMAT VALIDITY
# ---------------------------------------------------------------------------

# Regex patterns for common format checks
_DATE_PATTERNS = [
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),                    # ISO 8601: 2024-01-31
    re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$"),              # US: 1/31/2024
    re.compile(r"^\d{1,2}-\d{1,2}-\d{2,4}$"),              # Dashed: 31-01-2024
]

_CURRENCY_NOISE = re.compile(r"[$€£¥,\s]")   # Characters that shouldn't be in numeric cols
_AMOUNT_PATTERN = re.compile(r"^-?\d+(\.\d+)?$")           # Clean numeric after stripping noise

# Heuristic: column names that suggest they should contain numeric amounts
_AMOUNT_COL_HINTS = re.compile(
    r"(amount|spend|cost|price|fee|revenue|budget|total|balance|ytd|qty|quantity)",
    re.IGNORECASE,
)
_DATE_COL_HINTS = re.compile(
    r"(date|dt|_at|_on|timestamp|period|month|year|day)",
    re.IGNORECASE,
)


def _check_format_validity(
    df: pd.DataFrame,
    meta: DatasetMeta,
    **_,
) -> list[Issue]:
    issues: list[Issue] = []

    for col in df.columns:
        col_issues = _check_column_format(df, meta.dataset_id, col)
        issues.extend(col_issues)

    return issues


def _check_column_format(df: pd.DataFrame, dataset_id: str, col: str) -> list[Issue]:
    issues: list[Issue] = []
    series = df[col].dropna()

    if len(series) == 0:
        return issues

    # --- Amount columns: flag values that contain currency noise ---
    if _AMOUNT_COL_HINTS.search(col) and (pd.api.types.is_string_dtype(series) or series.dtype == object):
        dirty_rows = []
        for idx, val in series.items():
            s = str(val)
            # Strip known noise and check if remainder is a valid number
            cleaned = _CURRENCY_NOISE.sub("", s).strip()
            if cleaned and not _AMOUNT_PATTERN.match(cleaned):
                dirty_rows.append(idx)
            elif _CURRENCY_NOISE.search(s):
                # Value is parseable but has noise (e.g., "$1,500.00")
                dirty_rows.append(idx)

        if dirty_rows:
            issues.append(
                Issue(
                    dataset_id=dataset_id,
                    category=IssueCategory.FORMAT_VALIDITY,
                    severity=IssueSeverity.MEDIUM,
                    affected_row_indices=dirty_rows,
                    affected_columns=[col],
                    description=(
                        f"Column '{col}' appears to be a numeric/amount field but "
                        f"{len(dirty_rows)} row(s) contain currency symbols, commas, "
                        f"or other non-numeric characters (e.g. '$1,500.00'). "
                        f"These must be cleaned to a plain numeric format before loading."
                    ),
                    confidence=0.90,
                    raw_values={col: _safe_values(df, col, dirty_rows[:5])},
                    proposed_action=ProposedAction(
                        action_type=ActionType.SET_VALUE,
                        target_column=col,
                        target_row_indices=dirty_rows,
                        canonical_value="<stripped numeric>",
                        rationale=(
                            "Strip currency symbols and commas; cast column to float64. "
                            "Review each value before approving — some entries may be "
                            "non-numeric for a legitimate reason."
                        ),
                    ),
                )
            )

    # --- Date columns: flag values that look like dates but can't be parsed ---
    if _DATE_COL_HINTS.search(col) and (pd.api.types.is_string_dtype(series) or series.dtype == object):
        unparseable_rows = []
        for idx, val in series.items():
            if not _looks_like_date(str(val)):
                unparseable_rows.append(idx)

        if unparseable_rows:
            issues.append(
                Issue(
                    dataset_id=dataset_id,
                    category=IssueCategory.FORMAT_VALIDITY,
                    severity=IssueSeverity.MEDIUM,
                    affected_row_indices=unparseable_rows,
                    affected_columns=[col],
                    description=(
                        f"Column '{col}' appears to be a date field but "
                        f"{len(unparseable_rows)} row(s) contain values that cannot "
                        f"be parsed as a date. Mixed formats or free-text entries "
                        f"will cause errors in downstream date calculations."
                    ),
                    confidence=0.85,
                    raw_values={col: _safe_values(df, col, unparseable_rows[:5])},
                    proposed_action=ProposedAction(
                        action_type=ActionType.FLAG_ONLY,
                        target_column=col,
                        target_row_indices=unparseable_rows,
                        rationale=(
                            "Date values are ambiguous — automatic conversion could "
                            "silently mis-parse MM/DD vs DD/MM formats. "
                            "Review and standardise to ISO 8601 (YYYY-MM-DD)."
                        ),
                    ),
                )
            )

    return issues


def _looks_like_date(val: str) -> bool:
    """Return True if val matches any known date pattern OR is parseable by dateutil."""
    for pattern in _DATE_PATTERNS:
        if pattern.match(val.strip()):
            return True
    try:
        from dateutil import parser as dateutil_parser
        dateutil_parser.parse(val, fuzzy=False)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 5. REFERENTIAL INTEGRITY
# ---------------------------------------------------------------------------


def _check_referential_integrity(
    df: pd.DataFrame,
    meta: DatasetMeta,
    reference_sets: dict[str, set],
    **_,
) -> list[Issue]:
    """Flag values in a column that are not in a caller-supplied reference set."""
    issues: list[Issue] = []

    for col, valid_values in reference_sets.items():
        if col not in df.columns:
            logger.warning("REFERENTIAL_INTEGRITY: column '%s' not in DataFrame.", col)
            continue

        series = df[col].dropna()
        invalid_mask = ~series.isin(valid_values)
        invalid_rows = list(series.index[invalid_mask])

        if not invalid_rows:
            continue

        invalid_vals = series[invalid_mask].unique().tolist()

        issues.append(
            Issue(
                dataset_id=meta.dataset_id,
                category=IssueCategory.REFERENTIAL_INTEGRITY,
                severity=IssueSeverity.HIGH,
                affected_row_indices=invalid_rows,
                affected_columns=[col],
                description=(
                    f"Column '{col}' contains {len(invalid_rows)} value(s) not found "
                    f"in the reference set. Invalid values: "
                    f"{invalid_vals[:10]}{'...' if len(invalid_vals) > 10 else ''}. "
                    f"These records will fail validation when loaded into the target system."
                ),
                confidence=1.0,
                raw_values={col: _safe_values(df, col, invalid_rows[:10])},
                proposed_action=ProposedAction(
                    action_type=ActionType.FLAG_ONLY,
                    target_column=col,
                    target_row_indices=invalid_rows,
                    rationale=(
                        "The correct replacement value cannot be inferred automatically. "
                        "Each invalid entry must be mapped to a valid reference value manually."
                    ),
                ),
            )
        )

    return issues


# ---------------------------------------------------------------------------
# 6. CONSISTENCY
# ---------------------------------------------------------------------------


def _check_consistency(
    df: pd.DataFrame,
    meta: DatasetMeta,
    consistency_checks: list[tuple[list[str], str]],
    **_,
) -> list[Issue]:
    """
    For each (key_columns, value_column) pair: find rows where the same key
    maps to different values in value_column.

    Example: same tax_id maps to two different vendor_name values.
    """
    issues: list[Issue] = []

    for key_cols, val_col in consistency_checks:
        missing = [c for c in key_cols + [val_col] if c not in df.columns]
        if missing:
            logger.warning("CONSISTENCY: columns %s not found — skipping.", missing)
            continue

        # For each unique key, collect the set of distinct values in val_col
        grouped = (
            df.dropna(subset=key_cols)
            .groupby(key_cols, dropna=False)[val_col]
            .apply(lambda s: s.dropna().unique().tolist())
        )

        for key, values in grouped.items():
            if len(values) <= 1:
                continue

            # Find all row indices for this key
            mask = pd.Series([True] * len(df), index=df.index)
            if isinstance(key, tuple):
                for k_col, k_val in zip(key_cols, key):
                    mask &= df[k_col] == k_val
            else:
                mask &= df[key_cols[0]] == key

            row_indices = list(df.index[mask])
            key_str = str(key) if not isinstance(key, tuple) else ", ".join(str(k) for k in key)

            issues.append(
                Issue(
                    dataset_id=meta.dataset_id,
                    category=IssueCategory.CONSISTENCY,
                    severity=IssueSeverity.HIGH,
                    affected_row_indices=row_indices,
                    affected_columns=key_cols + [val_col],
                    description=(
                        f"Inconsistency in '{val_col}': the key ({key_str}) maps to "
                        f"{len(values)} different values: {values}. "
                        f"This suggests the same entity appears under multiple names "
                        f"or has conflicting attributes across records."
                    ),
                    confidence=1.0,
                    raw_values={val_col: _safe_values(df, val_col, row_indices)},
                    proposed_action=ProposedAction(
                        action_type=ActionType.FLAG_ONLY,
                        target_column=val_col,
                        target_row_indices=row_indices,
                        rationale=(
                            f"Multiple values exist for the same key. "
                            f"Analyst must choose the canonical value for '{val_col}'."
                        ),
                    ),
                )
            )

    return issues


# ---------------------------------------------------------------------------
# 7. OUTLIERS
# ---------------------------------------------------------------------------


def _check_outliers(
    df: pd.DataFrame,
    meta: DatasetMeta,
    **_,
) -> list[Issue]:
    """Flag numeric values more than OUTLIER_SIGMA standard deviations from the mean."""
    issues: list[Issue] = []

    numeric_cols = df.select_dtypes(include="number").columns

    for col in numeric_cols:
        series = df[col].dropna()
        if len(series) < OUTLIER_MIN_VALUES:
            continue

        mean = series.mean()
        std = series.std()
        if std == 0:
            continue

        z_scores = (series - mean) / std
        outlier_mask = z_scores.abs() > OUTLIER_SIGMA
        outlier_rows = list(series.index[outlier_mask])

        if not outlier_rows:
            continue

        outlier_values = series[outlier_mask].tolist()
        max_z = float(z_scores.abs()[outlier_mask].max())

        issues.append(
            Issue(
                dataset_id=meta.dataset_id,
                category=IssueCategory.OUTLIER,
                severity=IssueSeverity.MEDIUM,
                affected_row_indices=outlier_rows,
                affected_columns=[col],
                description=(
                    f"Column '{col}' has {len(outlier_rows)} value(s) more than "
                    f"{OUTLIER_SIGMA:.0f} standard deviations from the mean "
                    f"(mean={mean:,.2f}, std={std:,.2f}, max |z|={max_z:.1f}). "
                    f"Outlier values: {[round(v, 2) for v in outlier_values[:5]]}. "
                    f"These may be data entry errors or legitimate exceptional transactions."
                ),
                confidence=0.80,
                raw_values={col: [round(v, 4) for v in outlier_values[:10]]},
                proposed_action=ProposedAction(
                    action_type=ActionType.FLAG_ONLY,
                    target_column=col,
                    target_row_indices=outlier_rows,
                    rationale=(
                        "Outlier values require human judgement — they may be legitimate "
                        "(large contracts, one-time payments) or data entry errors. "
                        "No automatic fix is proposed."
                    ),
                ),
            )
        )

    return issues


# ---------------------------------------------------------------------------
# 8. DOMAIN RULES
# ---------------------------------------------------------------------------


def _check_domain_rules(
    df: pd.DataFrame,
    meta: DatasetMeta,
    domain_rules: list[DomainRule],
    **_,
) -> list[Issue]:
    """Evaluate each caller-supplied DomainRule against the DataFrame."""
    issues: list[Issue] = []

    for rule in domain_rules:
        if rule.column not in df.columns:
            logger.warning("DOMAIN_RULE: column '%s' not found — skipping.", rule.column)
            continue

        series = df[rule.column].dropna()
        failing_rows = []

        for idx, val in series.items():
            try:
                if not rule.rule_fn(val):
                    failing_rows.append(idx)
            except Exception:
                failing_rows.append(idx)  # Treat evaluation errors as failures

        if not failing_rows:
            continue

        if rule.fix_value is not None:
            action = ProposedAction(
                action_type=ActionType.SET_VALUE,
                target_column=rule.column,
                target_row_indices=failing_rows,
                canonical_value=rule.fix_value,
                rationale=f"Set non-compliant values to the specified fix value: {rule.fix_value!r}.",
            )
        else:
            action = ProposedAction(
                action_type=ActionType.FLAG_ONLY,
                target_column=rule.column,
                target_row_indices=failing_rows,
                rationale=f"Domain rule violated: {rule.description}. Manual correction required.",
            )

        issues.append(
            Issue(
                dataset_id=meta.dataset_id,
                category=IssueCategory.DOMAIN_RULE,
                severity=rule.severity,
                affected_row_indices=failing_rows,
                affected_columns=[rule.column],
                description=(
                    f"Domain rule violation in '{rule.column}': {rule.description} "
                    f"{len(failing_rows)} row(s) fail this rule."
                ),
                confidence=1.0,
                raw_values={rule.column: _safe_values(df, rule.column, failing_rows[:5])},
                proposed_action=action,
            )
        )

    return issues


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------


def _safe_values(df: pd.DataFrame, col: str, row_indices: list[int]) -> list[Any]:
    """Safely extract cell values for display, converting to plain Python types."""
    result = []
    for idx in row_indices:
        try:
            val = df.at[idx, col]
            # Convert pandas NA / numpy scalars to plain Python
            if pd.isna(val):
                result.append(None)
            elif hasattr(val, "item"):
                result.append(val.item())
            else:
                result.append(val)
        except Exception:
            result.append(None)
    return result


def _system_error_issue(dataset_id: str, category: IssueCategory, error_msg: str) -> Issue:
    """Produce a placeholder Issue when a check crashes, so the run completes."""
    return Issue(
        dataset_id=dataset_id,
        category=category,
        severity=IssueSeverity.LOW,
        description=(
            f"The {category.value} check encountered an internal error and could not complete. "
            f"Error: {error_msg[:200]}. "
            f"Other checks ran normally. Please review the application logs."
        ),
        confidence=1.0,
        proposed_action=ProposedAction(
            action_type=ActionType.FLAG_ONLY,
            rationale="Internal check error — no automated action possible.",
        ),
    )
