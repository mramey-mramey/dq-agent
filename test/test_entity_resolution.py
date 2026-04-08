"""
tests/test_entity_resolution.py

Unit tests for backend/tools/entity_resolution.py.

Coverage:
  SignalConfig         active_signals(), columns_for(), default weights
  Normalisation        _norm_name (legal suffix strip), _norm_tax_id, _norm_phone,
                       _norm_zip, _normalise_df (multi-col join, null → "")
  Pairwise scoring     name / tax_id / address / zip / phone signals;
                       empty-value skip (not penalised); evidence strings
  Union-Find           basic union/find, path compression,
                       transitive 3-way cluster, two independent clusters
  Cluster building     pairs → clusters; no pairs → empty; transitive merge
  Canonical election   most-complete row wins; tie → lower index; 3-way
  Canonical record     base row preserved; null filled from members;
                       most-common fill value; no fill when all null
  MergeCluster build   confidence, evidence_summary, retain/retire indices;
                       single row → None
  resolve_entities()   happy path; known pairs found; issue count = cluster count;
                       no-signal short-circuit; missing columns graceful;
                       pairs_evaluated math; transitive 3-way cluster;
                       unrelated rows not clustered; canonical_value in proposal;
                       summary text; fixture integration
  auto_detect_config   column name hinting; each column assigned once
  _validate_config     valid cols kept; missing cols removed; weights preserved
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

os.environ.setdefault("DQ_MAX_RECORDS_PER_RUN", "50000")
os.environ.setdefault("OUTPUT_DIR", tempfile.mkdtemp(prefix="dq_er_test_"))

from backend.models.dataset import ColumnMeta, DatasetMeta, FileFormat, SourceType
from backend.models.issue import ActionType, IssueCategory
from backend.tools.entity_resolution import (
    CLUSTER_HIGH_CONFIDENCE,
    CLUSTER_LOW_CONFIDENCE,
    SignalConfig,
    _UnionFind,
    _build_canonical_record,
    _build_clusters,
    _build_merge_cluster,
    _elect_canonical,
    _norm_name,
    _norm_phone,
    _norm_tax_id,
    _norm_zip,
    _normalise_df,
    _score_pair,
    _validate_config,
    auto_detect_config,
    resolve_entities,
)

rapidfuzz = pytest.importorskip("rapidfuzz")

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE_CSV = FIXTURES / "sample_vendor_data.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _meta(df: pd.DataFrame, dataset_id: str = "er-test-001") -> DatasetMeta:
    return DatasetMeta(
        dataset_id=dataset_id,
        source_type=SourceType.FILE,
        file_format=FileFormat.CSV,
        row_count=len(df),
        column_count=len(df.columns),
        columns=[
            ColumnMeta(
                name=str(c), dtype=str(df[c].dtype),
                nullable=bool(df[c].isna().any()),
                unique_count=int(df[c].nunique()),
                sample_values=[],
            )
            for c in df.columns
        ],
    )


def _vendor_df() -> pd.DataFrame:
    """Small DataFrame with two known duplicate pairs for deterministic tests."""
    return pd.DataFrame({
        "vendor_id":   ["V-001", "V-002", "V-003", "V-004", "V-005"],
        "vendor_name": [
            "Acme Corp.",
            "ACME Corporation",
            "Globex Supplies",
            "Globex Supplies Inc.",
            "Unrelated Widget Co.",
        ],
        "tax_id":  ["47-1234567", "47-1234567", "82-9876543", "82-9876543", "55-0011223"],
        "address": ["123 Main St",      "123 Main Street",   "456 Industrial Blvd",
                    "456 Industrial Blvd", "789 Office Park"],
        "city":    ["Springfield", "Springfield", "Shelbyville", "Shelbyville", "Capitol City"],
        "zip":     ["62701", "62701", "62565", "62565", "62702"],
    })


def _config_full() -> SignalConfig:
    return SignalConfig(
        name_columns=["vendor_name"],
        tax_id_columns=["tax_id"],
        address_columns=["address", "city"],
        zip_columns=["zip"],
    )


def _pair(row_a, row_b, composite=0.92):
    from backend.tools.entity_resolution import PairScore
    return PairScore(
        row_a=row_a, row_b=row_b, composite=composite,
        signal_scores={"name": composite},
        evidence=[f"Name similarity {composite*100:.0f}/100"],
    )


# ---------------------------------------------------------------------------
# SignalConfig
# ---------------------------------------------------------------------------


class TestSignalConfig:
    def test_active_signals_empty_when_no_columns(self):
        assert SignalConfig().active_signals() == []

    def test_active_signals_only_configured(self):
        cfg = SignalConfig(name_columns=["vendor_name"], zip_columns=["zip"])
        assert "name" in cfg.active_signals()
        assert "zip"  in cfg.active_signals()
        assert "tax_id" not in cfg.active_signals()

    def test_columns_for_returns_correct_list(self):
        cfg = SignalConfig(name_columns=["vendor_name", "dba_name"])
        assert cfg.columns_for("name") == ["vendor_name", "dba_name"]
        assert cfg.columns_for("tax_id") == []

    def test_default_weights_positive(self):
        cfg = SignalConfig(name_columns=["x"])
        assert cfg.weights.get("name", 0) > 0


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


class TestNormalisation:
    def test_norm_name_strips_llc(self):
        assert "llc" not in _norm_name("Acme LLC")

    def test_norm_name_strips_corporation(self):
        assert "corporation" not in _norm_name("ACME Corporation")

    def test_norm_name_strips_inc(self):
        assert "inc" not in _norm_name("Globex Supplies Inc.")

    def test_norm_name_lowercases(self):
        assert _norm_name("ACME") == "acme"

    def test_norm_name_null_empty(self):
        assert _norm_name(None) == ""
        assert _norm_name(float("nan")) == ""

    def test_norm_tax_id_strips_dashes(self):
        assert _norm_tax_id("47-1234567") == "471234567"

    def test_norm_tax_id_strips_spaces(self):
        assert _norm_tax_id("47 1234567") == "471234567"

    def test_norm_tax_id_uppercases(self):
        assert _norm_tax_id("ab-123") == "AB123"

    def test_norm_tax_id_null_empty(self):
        assert _norm_tax_id(None) == ""

    def test_norm_phone_last_10_digits(self):
        assert _norm_phone("+1-312-555-1234") == "3125551234"

    def test_norm_phone_strips_non_digits(self):
        assert _norm_phone("(312) 555-1234") == "3125551234"

    def test_norm_phone_null_empty(self):
        assert _norm_phone(None) == ""

    def test_norm_zip_first_5(self):
        assert _norm_zip("62701-1234") == "62701"

    def test_norm_zip_null_empty(self):
        assert _norm_zip(None) == ""

    def test_normalise_df_covers_all_signals(self):
        df  = _vendor_df()
        cfg = _config_full()
        res = _normalise_df(df, cfg)
        for signal in cfg.active_signals():
            assert signal in res
            assert len(res[signal]) == len(df)

    def test_normalise_df_multi_col_join(self):
        df  = _vendor_df()
        cfg = SignalConfig(address_columns=["address", "city"])
        res = _normalise_df(df, cfg)
        val = res["address"][0]
        assert "123" in val and "springfield" in val

    def test_normalise_df_null_becomes_empty(self):
        df  = pd.DataFrame({"vendor_name": [None]})
        cfg = SignalConfig(name_columns=["vendor_name"])
        res = _normalise_df(df, cfg)
        assert res["name"][0] == ""


# ---------------------------------------------------------------------------
# Pairwise scoring
# ---------------------------------------------------------------------------


class TestPairScoring:
    def _norm(self, df, cfg):
        return _normalise_df(df, cfg)

    def test_identical_rows_near_1(self):
        from rapidfuzz import fuzz
        df   = pd.DataFrame({"vendor_name": ["Acme Corp", "Acme Corp"],
                              "tax_id": ["471234567", "471234567"]})
        cfg  = SignalConfig(name_columns=["vendor_name"], tax_id_columns=["tax_id"])
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert pair.composite >= 0.95

    def test_different_rows_near_0(self):
        from rapidfuzz import fuzz
        df   = pd.DataFrame({"vendor_name": ["Acme Corp", "Zeta Dynamics"],
                              "tax_id": ["471234567", "990000001"]})
        cfg  = SignalConfig(name_columns=["vendor_name"], tax_id_columns=["tax_id"])
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert pair.composite < 0.30

    def test_tax_id_match_boosts_score(self):
        from rapidfuzz import fuzz
        df  = pd.DataFrame({"vendor_name": ["Acme Corp.", "ACME Corporation"],
                             "tax_id": ["471234567", "471234567"]})
        cfg = SignalConfig(name_columns=["vendor_name"], tax_id_columns=["tax_id"],
                           weights={"name": 0.4, "tax_id": 0.6})
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert pair.composite >= 0.70

    def test_empty_signal_not_penalised(self):
        from rapidfuzz import fuzz
        df  = pd.DataFrame({"vendor_name": ["Acme Corp", "Acme Corp"],
                             "tax_id": ["471234567", None]})
        cfg = SignalConfig(name_columns=["vendor_name"], tax_id_columns=["tax_id"],
                           weights={"name": 0.5, "tax_id": 0.5})
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert pair.composite >= 0.90   # name matches perfectly; missing tax_id skipped

    def test_evidence_populated_on_match(self):
        from rapidfuzz import fuzz
        df  = pd.DataFrame({"vendor_name": ["Acme Corp", "Acme Corp"],
                             "tax_id": ["471234567", "471234567"]})
        cfg = SignalConfig(name_columns=["vendor_name"], tax_id_columns=["tax_id"])
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert len(pair.evidence) >= 1

    def test_evidence_empty_on_no_match(self):
        from rapidfuzz import fuzz
        df  = pd.DataFrame({"vendor_name": ["Acme", "Zeta"],
                             "tax_id": ["111", "999"]})
        cfg = SignalConfig(name_columns=["vendor_name"], tax_id_columns=["tax_id"])
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert pair.evidence == []

    def test_zip_exact_match_scores_1(self):
        from rapidfuzz import fuzz
        df  = pd.DataFrame({"zip": ["62701", "62701"]})
        cfg = SignalConfig(zip_columns=["zip"])
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert pair.signal_scores.get("zip", 0) == 1.0

    def test_zip_mismatch_scores_0(self):
        from rapidfuzz import fuzz
        df  = pd.DataFrame({"zip": ["62701", "10001"]})
        cfg = SignalConfig(zip_columns=["zip"])
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert pair.signal_scores.get("zip", 0) == 0.0

    def test_row_indices_preserved(self):
        from rapidfuzz import fuzz
        df  = pd.DataFrame({"vendor_name": ["A", "B"]})
        cfg = SignalConfig(name_columns=["vendor_name"])
        pair = _score_pair(0, 1, self._norm(df, cfg), cfg, fuzz)
        assert pair.row_a == 0 and pair.row_b == 1


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------


class TestUnionFind:
    def test_find_returns_self_initially(self):
        uf = _UnionFind([1, 2, 3])
        assert uf.find(1) == 1

    def test_union_connects(self):
        uf = _UnionFind([1, 2])
        uf.union(1, 2)
        assert uf.find(1) == uf.find(2)

    def test_transitive_three_way(self):
        uf = _UnionFind([1, 2, 3])
        uf.union(1, 2)
        uf.union(2, 3)
        assert uf.find(1) == uf.find(3)

    def test_no_cross_contamination(self):
        uf = _UnionFind([1, 2, 3, 4])
        uf.union(1, 2)
        uf.union(3, 4)
        assert uf.find(1) != uf.find(3)

    def test_clusters_only_multi_member(self):
        uf = _UnionFind([1, 2, 3, 4])
        uf.union(1, 2)
        assert all(len(c) >= 2 for c in uf.clusters())

    def test_two_independent_clusters(self):
        uf = _UnionFind([1, 2, 3, 4, 5, 6])
        uf.union(1, 2)
        uf.union(3, 4)
        uf.union(4, 5)
        assert len(uf.clusters()) == 2

    def test_path_compression_consistent(self):
        uf = _UnionFind([1, 2, 3, 4])
        uf.union(1, 2); uf.union(2, 3); uf.union(3, 4)
        root = uf.find(4)
        assert all(uf.find(i) == root for i in [1, 2, 3])


# ---------------------------------------------------------------------------
# Cluster building
# ---------------------------------------------------------------------------


class TestBuildClusters:
    def test_single_pair_forms_cluster(self):
        clusters = _build_clusters([_pair(0, 1)])
        assert len(clusters) == 1 and sorted(clusters[0]) == [0, 1]

    def test_no_pairs_empty(self):
        assert _build_clusters([]) == []

    def test_transitive_three_way(self):
        clusters = _build_clusters([_pair(0, 1), _pair(1, 2)])
        assert len(clusters) == 1 and sorted(clusters[0]) == [0, 1, 2]

    def test_two_independent_clusters(self):
        clusters = _build_clusters([_pair(0, 1), _pair(2, 3)])
        assert len(clusters) == 2


# ---------------------------------------------------------------------------
# Canonical election
# ---------------------------------------------------------------------------


class TestElectCanonical:
    def test_most_complete_wins(self):
        df = pd.DataFrame({"name": ["A", "A"], "tax_id": [None, "123"],
                           "addr": [None, "x"]})
        assert _elect_canonical([0, 1], df) == 1

    def test_tie_lower_index_wins(self):
        df = pd.DataFrame({"name": ["A", "A"], "tax_id": ["1", "1"]})
        assert _elect_canonical([0, 1], df) == 0

    def test_three_way(self):
        df = pd.DataFrame({"name": ["A", "A", "A"],
                           "f1": [None, "x", "x"],
                           "f2": [None, None, "y"]})
        assert _elect_canonical([0, 1, 2], df) == 2


# ---------------------------------------------------------------------------
# Canonical record builder
# ---------------------------------------------------------------------------


class TestBuildCanonicalRecord:
    def test_canonical_row_fields_preserved(self):
        df  = pd.DataFrame({"name": ["Acme Corp.", "Acme"], "tax_id": ["X", "X"]})
        rec = _build_canonical_record([0, 1], 0, df)
        assert rec["name"] == "Acme Corp."

    def test_null_filled_from_other_row(self):
        df  = pd.DataFrame({"name": ["A", "A"], "email": [None, "a@b.com"]})
        rec = _build_canonical_record([0, 1], 0, df)
        assert rec["email"] == "a@b.com"

    def test_most_common_value_wins(self):
        df  = pd.DataFrame({"name": ["A"]*4,
                             "email": [None, "x@y.com", "x@y.com", "z@y.com"]})
        rec = _build_canonical_record([0, 1, 2, 3], 0, df)
        assert rec["email"] == "x@y.com"

    def test_no_fill_when_all_null(self):
        df  = pd.DataFrame({"name": ["A", "A"], "email": [None, None]})
        rec = _build_canonical_record([0, 1], 0, df)
        # Value should be null/NaN
        import pandas as _pd
        assert rec["email"] is None or _pd.isna(rec["email"])


# ---------------------------------------------------------------------------
# MergeCluster build
# ---------------------------------------------------------------------------


class TestBuildMergeCluster:
    def test_cluster_id_is_string(self):
        df      = _vendor_df()
        cluster = _build_merge_cluster([0, 1], df, [_pair(0, 1)])
        assert isinstance(cluster.cluster_id, str)

    def test_confidence_max_pair_score(self):
        df      = _vendor_df()
        cluster = _build_merge_cluster([0, 1], df, [_pair(0, 1, 0.92)])
        assert cluster.confidence == 0.92

    def test_retain_most_complete(self):
        df = pd.DataFrame({"name": ["A", "A"], "email": [None, "x@y.com"]})
        cluster = _build_merge_cluster([0, 1], df, [_pair(0, 1)])
        assert cluster.retain_index == 1

    def test_retire_excludes_retain(self):
        df      = _vendor_df()
        cluster = _build_merge_cluster([0, 1], df, [_pair(0, 1)])
        assert cluster.retain_index not in cluster.retire_indices

    def test_evidence_summary_nonempty(self):
        df      = _vendor_df()
        cluster = _build_merge_cluster([0, 1], df, [_pair(0, 1)])
        assert len(cluster.evidence_summary) > 0

    def test_single_row_returns_none(self):
        assert _build_merge_cluster([0], _vendor_df(), []) is None


# ---------------------------------------------------------------------------
# resolve_entities — integration
# ---------------------------------------------------------------------------


class TestResolveEntities:
    def test_returns_result_object(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        result = resolve_entities(df, meta, _config_full())
        assert result.dataset_id == meta.dataset_id

    def test_finds_both_known_clusters(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        result = resolve_entities(df, meta, _config_full())
        assert len(result.clusters) >= 2

    def test_issue_count_equals_cluster_count(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        result = resolve_entities(df, meta, _config_full())
        assert len(result.issues) == len(result.clusters)

    def test_issues_are_deduplication_category(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        for issue in resolve_entities(df, meta, _config_full()).issues:
            assert issue.category == IssueCategory.DEDUPLICATION

    def test_high_confidence_gets_merge_proposal(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        result = resolve_entities(df, meta, _config_full())
        assert any(i.proposed_action.action_type == ActionType.MERGE_ROWS
                   for i in result.issues)

    def test_dedup_never_bulk_approvable(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        for issue in resolve_entities(df, meta, _config_full()).issues:
            assert issue.can_bulk_approve(bulk_threshold=0.99) is False

    def test_no_signals_short_circuits(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        result   = resolve_entities(df, meta, SignalConfig())
        assert len(result.clusters) == 0
        assert "skipped" in result.summary.lower()

    def test_missing_columns_graceful(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        result   = resolve_entities(df, meta, SignalConfig(name_columns=["nonexistent"]))
        assert result is not None

    def test_pairs_evaluated_count(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        n        = len(df)
        result   = resolve_entities(df, meta, _config_full())
        assert result.pairs_evaluated == n * (n - 1) // 2

    def test_summary_contains_dataset_id(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        result   = resolve_entities(df, meta, _config_full())
        assert meta.dataset_id in result.summary

    def test_transitive_three_way_cluster(self):
        df = pd.DataFrame({
            "vendor_name": ["Acme Corp.", "ACME Corporation", "Acme Co."],
            "tax_id":      ["471234567",  "471234567",        "471234567"],
            "address":     ["123 Main St","123 Main Street",  "123 Main St"],
            "zip":         ["62701",      "62701",            "62701"],
        })
        meta   = _meta(df)
        cfg    = SignalConfig(name_columns=["vendor_name"], tax_id_columns=["tax_id"],
                              address_columns=["address"], zip_columns=["zip"])
        result = resolve_entities(df, meta, cfg)
        assert len(result.clusters) == 1
        assert sorted(result.clusters[0].row_indices) == [0, 1, 2]

    def test_unrelated_rows_not_clustered(self):
        df = pd.DataFrame({
            "vendor_name": ["Acme Corp",   "Zeta Dynamics",  "Alpha Services"],
            "tax_id":      ["111111111",   "222222222",      "333333333"],
            "zip":         ["62701",       "10001",          "90210"],
        })
        meta   = _meta(df)
        cfg    = SignalConfig(name_columns=["vendor_name"], tax_id_columns=["tax_id"],
                              zip_columns=["zip"])
        result = resolve_entities(df, meta, cfg)
        assert len(result.clusters) == 0

    def test_canonical_value_is_dict_in_merge_proposal(self):
        df, meta = _vendor_df(), _meta(_vendor_df())
        result   = resolve_entities(df, meta, _config_full())
        for issue in result.issues:
            if issue.proposed_action.action_type == ActionType.MERGE_ROWS:
                assert isinstance(issue.proposed_action.canonical_value, dict)

    def test_fixture_integration(self):
        df   = pd.read_csv(SAMPLE_CSV)
        meta = _meta(df)
        cfg  = auto_detect_config(df)
        assert len(cfg.active_signals()) >= 1
        result = resolve_entities(df, meta, cfg)
        assert len(result.clusters) >= 4


# ---------------------------------------------------------------------------
# auto_detect_config
# ---------------------------------------------------------------------------


class TestAutoDetectConfig:
    def test_detects_vendor_name(self):
        cfg = auto_detect_config(pd.DataFrame({"vendor_name": [], "amount": []}))
        assert "vendor_name" in cfg.name_columns

    def test_detects_tax_id(self):
        cfg = auto_detect_config(pd.DataFrame({"tax_id": [], "name": []}))
        assert "tax_id" in cfg.tax_id_columns

    def test_detects_zip(self):
        cfg = auto_detect_config(pd.DataFrame({"zip_code": [], "name": []}))
        assert "zip_code" in cfg.zip_columns

    def test_detects_address_cols(self):
        cfg = auto_detect_config(pd.DataFrame({"address": [], "city": [], "state": []}))
        for col in ["address", "city", "state"]:
            assert col in cfg.address_columns

    def test_unrecognised_not_assigned(self):
        cfg = auto_detect_config(pd.DataFrame({"foobar": [], "xyz_code": []}))
        assert cfg.active_signals() == []

    def test_each_col_assigned_at_most_once(self):
        df  = _vendor_df()
        cfg = auto_detect_config(df)
        all_cols = (cfg.name_columns + cfg.tax_id_columns + cfg.address_columns
                    + cfg.zip_columns + cfg.phone_columns)
        assert len(all_cols) == len(set(all_cols))


# ---------------------------------------------------------------------------
# _validate_config
# ---------------------------------------------------------------------------


class TestValidateConfig:
    def test_valid_columns_kept(self):
        validated = _validate_config(_config_full(), _vendor_df())
        assert "vendor_name" in validated.name_columns

    def test_missing_columns_removed(self):
        cfg       = SignalConfig(name_columns=["nonexistent"])
        validated = _validate_config(cfg, _vendor_df())
        assert "nonexistent" not in validated.name_columns

    def test_weights_preserved(self):
        cfg       = SignalConfig(name_columns=["vendor_name"], weights={"name": 0.9})
        validated = _validate_config(cfg, _vendor_df())
        assert validated.weights["name"] == 0.9