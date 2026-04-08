"""
backend/tools/entity_resolution.py

Entity Resolution Engine for the DQ agent.

Where quality_checks._check_deduplication does single-column pairwise fuzzy
matching, this module goes deeper:

  1. Multi-signal scoring   — combine name, address, tax ID, phone, zip into a
                              weighted composite score per row-pair.
  2. Transitive clustering  — if A~B and B~C they form cluster {A,B,C} via
                              Union-Find.
  3. Canonical record build — elect the most-complete row; fill nulls from
                              other cluster members (best-of merge).
  4. Issue generation       — one DEDUPLICATION Issue per cluster.

Entry point:
    resolve_entities(df, meta, config) -> EntityResolutionResult

READ-ONLY — never modifies the DataFrame.
Dependencies: rapidfuzz, pandas
"""

from __future__ import annotations

import logging
import re
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from backend.models.dataset import DatasetMeta
from backend.models.issue import (
    ActionType,
    Issue,
    IssueCategory,
    IssueSeverity,
    ProposedAction,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

DEFAULT_SIGNAL_WEIGHTS: dict[str, float] = {
    "name":    0.40,
    "tax_id":  0.30,
    "address": 0.15,
    "zip":     0.08,
    "phone":   0.07,
}

CLUSTER_HIGH_CONFIDENCE = 0.88   # Merge proposal generated
CLUSTER_LOW_CONFIDENCE  = 0.72   # Flagged for human review
FUZZY_MATCH_CUTOFF      = 72     # Min fuzzy score (0-100) to count as evidence


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------


@dataclass
class SignalConfig:
    """
    Declares which DataFrame columns carry which entity-matching signals.

    Example:
        SignalConfig(
            name_columns=["vendor_name"],
            tax_id_columns=["tax_id"],
            address_columns=["address", "city"],
            zip_columns=["zip"],
        )
    """
    name_columns:    list[str] = field(default_factory=list)
    tax_id_columns:  list[str] = field(default_factory=list)
    address_columns: list[str] = field(default_factory=list)
    zip_columns:     list[str] = field(default_factory=list)
    phone_columns:   list[str] = field(default_factory=list)
    weights:         dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_SIGNAL_WEIGHTS)
    )

    def active_signals(self) -> list[str]:
        return [s for s in ("name", "tax_id", "address", "zip", "phone")
                if self.columns_for(s)]

    def columns_for(self, signal: str) -> list[str]:
        return {
            "name":    self.name_columns,
            "tax_id":  self.tax_id_columns,
            "address": self.address_columns,
            "zip":     self.zip_columns,
            "phone":   self.phone_columns,
        }.get(signal, [])


@dataclass
class PairScore:
    """Composite score between two row indices."""
    row_a:         int
    row_b:         int
    composite:     float
    signal_scores: dict[str, float]
    evidence:      list[str]


@dataclass
class MergeCluster:
    """A group of rows believed to represent the same real-world entity."""
    cluster_id:       str
    row_indices:      list[int]
    retain_index:     int
    retire_indices:   list[int]
    canonical_record: dict[str, Any]
    confidence:       float
    evidence_summary: str


@dataclass
class EntityResolutionResult:
    """Full output of resolve_entities()."""
    dataset_id:            str
    clusters:              list[MergeCluster]
    issues:                list[Issue]
    pairs_evaluated:       int
    pairs_above_threshold: int
    summary:               str


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def resolve_entities(
    df: pd.DataFrame,
    meta: DatasetMeta,
    config: SignalConfig,
) -> EntityResolutionResult:
    """
    Run full entity resolution. Steps:
      1. Validate config columns.
      2. Normalise field strings.
      3. Score all row pairs.
      4. Build transitive clusters (Union-Find).
      5. Elect canonical record per cluster.
      6. Generate one Issue per cluster.
    """
    try:
        from rapidfuzz import fuzz
    except ImportError:
        logger.warning("rapidfuzz not installed. Run: pip install rapidfuzz")
        return EntityResolutionResult(
            dataset_id=meta.dataset_id, clusters=[], issues=[],
            pairs_evaluated=0, pairs_above_threshold=0,
            summary="Entity resolution skipped — rapidfuzz not installed.",
        )

    if not config.active_signals():
        return EntityResolutionResult(
            dataset_id=meta.dataset_id, clusters=[], issues=[],
            pairs_evaluated=0, pairs_above_threshold=0,
            summary="Entity resolution skipped — no signals configured.",
        )

    config = _validate_config(config, df)
    if not config.active_signals():
        return EntityResolutionResult(
            dataset_id=meta.dataset_id, clusters=[], issues=[],
            pairs_evaluated=0, pairs_above_threshold=0,
            summary="Entity resolution skipped — no configured columns found in DataFrame.",
        )

    normalised       = _normalise_df(df, config)
    pairs, n_eval    = _score_all_pairs(normalised, config, fuzz)
    above            = [p for p in pairs if p.composite >= CLUSTER_LOW_CONFIDENCE]
    raw_clusters     = _build_clusters(above)
    clusters         = [c for c in
                        (_build_merge_cluster(ri, df, pairs) for ri in raw_clusters)
                        if c is not None]
    issues           = [_cluster_to_issue(c, meta.dataset_id) for c in clusters]

    n_high = sum(1 for c in clusters if c.confidence >= CLUSTER_HIGH_CONFIDENCE)
    n_low  = len(clusters) - n_high
    summary = (
        f"Entity resolution complete for dataset {meta.dataset_id}. "
        f"Evaluated {n_eval:,} row pair(s). "
        f"Found {len(above)} pair(s) above threshold, "
        f"grouped into {len(clusters)} cluster(s): "
        f"{n_high} with merge proposal(s), {n_low} flagged for human review."
    )
    logger.info(summary)
    return EntityResolutionResult(
        dataset_id=meta.dataset_id,
        clusters=clusters, issues=issues,
        pairs_evaluated=n_eval,
        pairs_above_threshold=len(above),
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

_PUNCT      = re.compile(r"[^\w\s]")
_WHITESPACE = re.compile(r"\s+")
_NON_DIGIT  = re.compile(r"\D")
_LEGAL_SFXS = re.compile(
    r"\b(inc|incorporated|llc|ltd|limited|corp|corporation|co|company|"
    r"lp|llp|plc|gmbh|ag|sa|sas|bv|nv|pty|pvt)\b\.?$",
    re.IGNORECASE,
)


def _norm_generic(val: Any) -> str:
    if pd.isna(val) or val is None:
        return ""
    s = str(val).lower().strip()
    s = _PUNCT.sub(" ", s)
    return _WHITESPACE.sub(" ", s).strip()


def _norm_name(val: Any) -> str:
    s = _norm_generic(val)
    return _LEGAL_SFXS.sub("", s).strip()


def _norm_tax_id(val: Any) -> str:
    if pd.isna(val) or val is None:
        return ""
    return re.sub(r"[\s\-]", "", str(val)).upper()


def _norm_phone(val: Any) -> str:
    if pd.isna(val) or val is None:
        return ""
    d = _NON_DIGIT.sub("", str(val))
    return d[-10:] if len(d) > 10 else d


def _norm_zip(val: Any) -> str:
    if pd.isna(val) or val is None:
        return ""
    return _NON_DIGIT.sub("", str(val))[:5]


_NORM_FN: dict[str, Any] = {
    "name":    _norm_name,
    "tax_id":  _norm_tax_id,
    "address": _norm_generic,
    "zip":     _norm_zip,
    "phone":   _norm_phone,
}


def _normalise_df(df: pd.DataFrame, config: SignalConfig) -> dict[str, dict[int, str]]:
    """Pre-compute normalised string for every (signal, row)."""
    result: dict[str, dict[int, str]] = {}
    for signal in config.active_signals():
        fn   = _NORM_FN[signal]
        cols = [c for c in config.columns_for(signal) if c in df.columns]
        row_vals: dict[int, str] = {}
        for idx in df.index:
            parts    = [fn(df.at[idx, c]) for c in cols]
            combined = " ".join(p for p in parts if p).strip()
            row_vals[idx] = combined
        result[signal] = row_vals
    return result


# ---------------------------------------------------------------------------
# Pairwise scoring
# ---------------------------------------------------------------------------


def _score_pair(
    row_a: int,
    row_b: int,
    normalised: dict[str, dict[int, str]],
    config: SignalConfig,
    fuzz,
) -> PairScore:
    """
    Weighted composite score for one row pair.
    name/address: fuzzy; tax_id/zip/phone: exact match only.
    Empty values on either side → signal skipped, not penalised.
    """
    signal_scores:  dict[str, float] = {}
    active_weights: dict[str, float] = {}
    evidence:       list[str]        = []

    for signal in config.active_signals():
        val_a = normalised[signal].get(row_a, "")
        val_b = normalised[signal].get(row_b, "")
        if not val_a or not val_b:
            continue

        w = config.weights.get(signal, 0.0)
        active_weights[signal] = w

        if signal == "name":
            raw   = max(fuzz.token_set_ratio(val_a, val_b),
                        fuzz.partial_ratio(val_a, val_b))
            score = raw / 100.0
            if raw >= FUZZY_MATCH_CUTOFF:
                evidence.append(f"Name similarity {raw:.0f}/100 ('{val_a}' ~ '{val_b}')")
        elif signal == "address":
            raw   = fuzz.token_set_ratio(val_a, val_b)
            score = raw / 100.0
            if raw >= FUZZY_MATCH_CUTOFF:
                evidence.append(f"Address similarity {raw:.0f}/100")
        else:  # tax_id, zip, phone
            score = 1.0 if val_a == val_b else 0.0
            if score == 1.0:
                label = signal.replace("_", " ").title()
                evidence.append(f"{label} exact match: '{val_a}'")

        signal_scores[signal] = score

    total_w   = sum(active_weights.values())
    composite = (
        sum(signal_scores.get(s, 0.0) * w / total_w for s, w in active_weights.items())
        if total_w > 0 else 0.0
    )

    return PairScore(
        row_a=row_a, row_b=row_b,
        composite=round(composite, 4),
        signal_scores=signal_scores,
        evidence=evidence,
    )


def _score_all_pairs(
    normalised: dict[str, dict[int, str]],
    config: SignalConfig,
    fuzz,
) -> tuple[list[PairScore], int]:
    """Score all O(n²) row pairs."""
    indices = list(next(iter(normalised.values())).keys())
    n       = len(indices)
    pairs: list[PairScore] = []
    n_eval  = 0
    for i in range(n):
        for j in range(i + 1, n):
            n_eval += 1
            pairs.append(_score_pair(indices[i], indices[j], normalised, config, fuzz))
    return pairs, n_eval


# ---------------------------------------------------------------------------
# Transitive clustering — Union-Find
# ---------------------------------------------------------------------------


class _UnionFind:
    """Path-compressed, union-by-rank Union-Find."""

    def __init__(self, elements: list[int]) -> None:
        self._parent = {e: e for e in elements}
        self._rank   = {e: 0 for e in elements}

    def find(self, x: int) -> int:
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, x: int, y: int) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1

    def clusters(self) -> list[list[int]]:
        groups: dict[int, list[int]] = {}
        for e in self._parent:
            groups.setdefault(self.find(e), []).append(e)
        return [sorted(v) for v in groups.values() if len(v) > 1]


def _build_clusters(pairs: list[PairScore]) -> list[list[int]]:
    all_indices: set[int] = set()
    for p in pairs:
        all_indices.add(p.row_a)
        all_indices.add(p.row_b)
    if not all_indices:
        return []
    uf = _UnionFind(list(all_indices))
    for p in pairs:
        uf.union(p.row_a, p.row_b)
    return uf.clusters()


# ---------------------------------------------------------------------------
# Canonical record election
# ---------------------------------------------------------------------------


def _elect_canonical(row_indices: list[int], df: pd.DataFrame) -> int:
    """Return row index of the most-complete record; ties → lower index."""
    best_idx, best_score = row_indices[0], -1
    for idx in row_indices:
        score = sum(
            1 for col in df.columns
            if not pd.isna(df.at[idx, col]) and str(df.at[idx, col]).strip() != ""
        )
        if score > best_score:
            best_score, best_idx = score, idx
    return best_idx


def _build_canonical_record(
    row_indices: list[int],
    retain_index: int,
    df: pd.DataFrame,
) -> dict[str, Any]:
    """Best-of merge: start from canonical row; fill nulls from other members."""
    canonical: dict[str, Any] = {}
    for col in df.columns:
        base = df.at[retain_index, col]
        if not pd.isna(base) and str(base).strip() != "":
            canonical[col] = base
            continue
        candidates = [
            df.at[idx, col] for idx in row_indices
            if idx != retain_index
            and not pd.isna(df.at[idx, col])
            and str(df.at[idx, col]).strip() != ""
        ]
        if not candidates:
            canonical[col] = base
        else:
            most_common = Counter(str(c) for c in candidates).most_common(1)[0][0]
            canonical[col] = next(c for c in candidates if str(c) == most_common)
    return canonical


def _build_merge_cluster(
    row_indices: list[int],
    df: pd.DataFrame,
    all_pairs: list[PairScore],
) -> MergeCluster | None:
    if len(row_indices) < 2:
        return None

    cluster_set  = set(row_indices)
    inner_pairs  = [p for p in all_pairs
                    if p.row_a in cluster_set and p.row_b in cluster_set]
    confidence   = max((p.composite for p in inner_pairs), default=0.0)

    seen: set[str] = set()
    unique_ev: list[str] = []
    for p in inner_pairs:
        for e in p.evidence:
            if e not in seen:
                seen.add(e)
                unique_ev.append(e)

    retain_index     = _elect_canonical(row_indices, df)
    retire_indices   = [i for i in row_indices if i != retain_index]
    canonical_record = _build_canonical_record(row_indices, retain_index, df)

    if unique_ev:
        ev_str = "; ".join(unique_ev[:5])
        if len(unique_ev) > 5:
            ev_str += f" (and {len(unique_ev) - 5} more)"
    else:
        ev_str = f"Composite similarity: {confidence:.0%}"

    return MergeCluster(
        cluster_id=str(uuid.uuid4())[:8].upper(),
        row_indices=sorted(row_indices),
        retain_index=retain_index,
        retire_indices=retire_indices,
        canonical_record=canonical_record,
        confidence=round(confidence, 4),
        evidence_summary=ev_str,
    )


# ---------------------------------------------------------------------------
# Issue generation
# ---------------------------------------------------------------------------


def _cluster_to_issue(cluster: MergeCluster, dataset_id: str) -> Issue:
    """One DEDUPLICATION Issue per cluster."""
    is_high  = cluster.confidence >= CLUSTER_HIGH_CONFIDENCE
    severity = IssueSeverity.HIGH if is_high else IssueSeverity.MEDIUM
    conf_pct = f"{cluster.confidence:.0%}"
    n        = len(cluster.row_indices)

    description = (
        f"Entity resolution found {n} record(s) that likely represent the same "
        f"real-world entity (confidence: {conf_pct}). "
        f"Row indices: {cluster.row_indices}. "
        f"Evidence: {cluster.evidence_summary}. "
        f"Proposed canonical row: {cluster.retain_index}."
    )

    if is_high:
        action = ProposedAction(
            action_type=ActionType.MERGE_ROWS,
            retain_row_index=cluster.retain_index,
            retire_row_indices=cluster.retire_indices,
            canonical_value=cluster.canonical_record,
            rationale=(
                f"Confidence {conf_pct} exceeds auto-proposal threshold "
                f"({CLUSTER_HIGH_CONFIDENCE:.0%}). "
                f"Row {cluster.retain_index} elected as canonical. "
                f"Evidence: {cluster.evidence_summary}."
            ),
        )
    else:
        action = ProposedAction(
            action_type=ActionType.FLAG_ONLY,
            retain_row_index=cluster.retain_index,
            retire_row_indices=cluster.retire_indices,
            rationale=(
                f"Confidence {conf_pct} is above detection threshold "
                f"({CLUSTER_LOW_CONFIDENCE:.0%}) but below auto-proposal threshold "
                f"({CLUSTER_HIGH_CONFIDENCE:.0%}). Human review required."
            ),
        )

    return Issue(
        dataset_id=dataset_id,
        category=IssueCategory.DEDUPLICATION,
        severity=severity,
        affected_row_indices=cluster.row_indices,
        affected_columns=[],
        description=description,
        confidence=cluster.confidence,
        raw_values={},
        proposed_action=action,
    )


# ---------------------------------------------------------------------------
# Auto-detect signal columns from column names
# ---------------------------------------------------------------------------

_COL_HINTS: dict[str, re.Pattern] = {
    "name":    re.compile(r"(name|vendor|supplier|customer|company|entity)", re.IGNORECASE),
    "tax_id":  re.compile(r"(tax.?id|ein|tin|fein|vat|abn|gst)", re.IGNORECASE),
    "address": re.compile(r"(address|addr|street|city|state|province)", re.IGNORECASE),
    "zip":     re.compile(r"(zip|postal|post.?code)", re.IGNORECASE),
    "phone":   re.compile(r"(phone|tel|mobile|fax|contact)", re.IGNORECASE),
}


def auto_detect_config(df: pd.DataFrame) -> SignalConfig:
    """Heuristically build a SignalConfig from column names."""
    mapping: dict[str, list[str]] = {s: [] for s in _COL_HINTS}
    for col in df.columns:
        for signal, pattern in _COL_HINTS.items():
            if pattern.search(col):
                mapping[signal].append(col)
                break
    return SignalConfig(
        name_columns=mapping["name"],
        tax_id_columns=mapping["tax_id"],
        address_columns=mapping["address"],
        zip_columns=mapping["zip"],
        phone_columns=mapping["phone"],
    )


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def _validate_config(config: SignalConfig, df: pd.DataFrame) -> SignalConfig:
    """Drop column references not present in the DataFrame."""
    def _filter(cols: list[str]) -> list[str]:
        valid = []
        for c in cols:
            if c in df.columns:
                valid.append(c)
            else:
                logger.warning(
                    "Entity resolution: column '%s' not in DataFrame — skipping.", c
                )
        return valid

    return SignalConfig(
        name_columns=_filter(config.name_columns),
        tax_id_columns=_filter(config.tax_id_columns),
        address_columns=_filter(config.address_columns),
        zip_columns=_filter(config.zip_columns),
        phone_columns=_filter(config.phone_columns),
        weights=config.weights,
    )