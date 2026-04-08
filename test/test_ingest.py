"""
tests/test_ingest.py

Unit tests for backend/tools/ingest.py.

Covers:
  - Mode A (file): CSV happy path, Excel happy path, unsupported extension,
    missing file, row limit enforcement, output filename derivation,
    column metadata inference, original file immutability
  - Mode B (DB): SQLite happy path, custom query, invalid table name,
    row limit enforcement, credential scrubbing in error messages,
    provenance extraction (no credentials in DatasetMeta)
  - Shared: DatasetMeta field population, ColumnMeta accuracy
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Patch MAX_RECORDS before importing ingest so we can test the limit cheaply
# ---------------------------------------------------------------------------
os.environ.setdefault("DQ_MAX_RECORDS_PER_RUN", "50000")
os.environ.setdefault("OUTPUT_DIR", tempfile.mkdtemp(prefix="dq_test_outputs_"))

from backend.models.dataset import FileFormat, SourceType  # noqa: E402
from backend.tools.ingest import (  # noqa: E402
    MAX_RECORDS,
    ingest_db_table,
    ingest_file,
)

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE_CSV = FIXTURES / "sample_vendor_data.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_temp_csv(rows: list[dict], suffix: str = ".csv") -> Path:
    """Write a small DataFrame to a temp file and return its path."""
    df = pd.DataFrame(rows)
    tmp = Path(tempfile.mktemp(suffix=suffix))
    if suffix == ".csv":
        df.to_csv(tmp, index=False)
    else:
        df.to_excel(tmp, index=False)
    return tmp


def _make_sqlite_db(rows: list[dict], table: str = "vendors") -> tuple[str, str]:
    """
    Create an in-memory-style SQLite file with the given rows.
    Returns (connection_string, db_path_str).
    """
    import sqlite3

    tmp_db = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(tmp_db)
    df = pd.DataFrame(rows)
    df.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()
    return f"sqlite:///{tmp_db}", tmp_db


# ---------------------------------------------------------------------------
# Mode A — CSV
# ---------------------------------------------------------------------------


class TestIngestFileCSV:
    def test_happy_path_returns_success(self):
        result = ingest_file(str(SAMPLE_CSV))
        assert result.success is True
        assert result.error is None

    def test_dataframe_shape_matches_file(self):
        result = ingest_file(str(SAMPLE_CSV))
        expected_df = pd.read_csv(SAMPLE_CSV)
        assert result.dataframe is not None
        assert len(result.dataframe) == len(expected_df)
        assert list(result.dataframe.columns) == list(expected_df.columns)

    def test_dataset_meta_source_type(self):
        result = ingest_file(str(SAMPLE_CSV))
        assert result.dataset_meta.source_type == SourceType.FILE

    def test_dataset_meta_file_format_csv(self):
        result = ingest_file(str(SAMPLE_CSV))
        assert result.dataset_meta.file_format == FileFormat.CSV

    def test_dataset_meta_row_and_column_counts(self):
        result = ingest_file(str(SAMPLE_CSV))
        meta = result.dataset_meta
        assert meta.row_count == len(result.dataframe)
        assert meta.column_count == len(result.dataframe.columns)

    def test_dataset_id_is_uuid_string(self):
        import uuid

        result = ingest_file(str(SAMPLE_CSV))
        uuid.UUID(result.dataset_meta.dataset_id)  # raises if invalid

    def test_output_filename_derived_correctly(self):
        result = ingest_file(str(SAMPLE_CSV))
        fn = result.dataset_meta.output_filename
        assert fn is not None
        assert fn.startswith("sample_vendor_data_clean_")
        assert fn.endswith(".csv")

    def test_original_file_not_modified(self):
        original_mtime = SAMPLE_CSV.stat().st_mtime
        ingest_file(str(SAMPLE_CSV))
        assert SAMPLE_CSV.stat().st_mtime == original_mtime

    def test_temp_file_path_is_not_original(self):
        result = ingest_file(str(SAMPLE_CSV))
        assert result.dataset_meta.temp_file_path != str(SAMPLE_CSV)
        assert Path(result.dataset_meta.temp_file_path).exists()

    def test_column_meta_populated(self):
        result = ingest_file(str(SAMPLE_CSV))
        cols = result.dataset_meta.columns
        assert len(cols) == result.dataset_meta.column_count
        names = [c.name for c in cols]
        assert "vendor_name" in names
        assert "vendor_id" in names

    def test_nullable_detected(self):
        result = ingest_file(str(SAMPLE_CSV))
        # address column has blanks in fixture
        address_col = result.dataset_meta.column_by_name("address")
        assert address_col is not None
        assert address_col.nullable is True

    def test_non_nullable_column(self):
        result = ingest_file(str(SAMPLE_CSV))
        # vendor_id has no nulls in fixture
        id_col = result.dataset_meta.column_by_name("vendor_id")
        assert id_col is not None
        assert id_col.nullable is False

    def test_sample_values_populated(self):
        result = ingest_file(str(SAMPLE_CSV))
        name_col = result.dataset_meta.column_by_name("vendor_name")
        assert len(name_col.sample_values) > 0

    def test_summary_contains_dataset_id(self):
        result = ingest_file(str(SAMPLE_CSV))
        assert result.dataset_meta.dataset_id in result.summary

    def test_missing_file_returns_failure(self):
        result = ingest_file("/nonexistent/path/file.csv")
        assert result.success is False
        assert result.error is not None
        assert result.dataframe is None

    def test_unsupported_extension_returns_failure(self):
        tmp = Path(tempfile.mktemp(suffix=".txt"))
        tmp.write_text("col1,col2\n1,2\n")
        try:
            result = ingest_file(str(tmp))
            assert result.success is False
            assert ".txt" in result.error
        finally:
            tmp.unlink(missing_ok=True)

    def test_row_limit_enforced(self, monkeypatch):
        monkeypatch.setattr("backend.tools.ingest.MAX_RECORDS", 5)
        result = ingest_file(str(SAMPLE_CSV))  # fixture has 15 rows
        assert result.success is False
        assert "exceeds the maximum" in result.error


# ---------------------------------------------------------------------------
# Mode A — Excel
# ---------------------------------------------------------------------------


class TestIngestFileExcel:
    def test_happy_path_xlsx(self):
        rows = [{"id": i, "name": f"Vendor {i}", "amount": i * 100.0} for i in range(1, 6)]
        tmp = _make_temp_csv(rows, suffix=".xlsx")
        try:
            result = ingest_file(str(tmp))
            assert result.success is True
            assert result.dataset_meta.file_format == FileFormat.XLSX
            assert len(result.dataframe) == 5
        finally:
            tmp.unlink(missing_ok=True)

    def test_default_sheet_used_when_none(self):
        rows = [{"col": "a"}, {"col": "b"}]
        tmp = _make_temp_csv(rows, suffix=".xlsx")
        try:
            result = ingest_file(str(tmp), sheet_name=None)
            assert result.success is True
            # sheet_name should be set to the actual first sheet name
            assert result.dataset_meta.sheet_name is not None
        finally:
            tmp.unlink(missing_ok=True)

    def test_invalid_sheet_name_returns_failure(self):
        rows = [{"col": "a"}]
        tmp = _make_temp_csv(rows, suffix=".xlsx")
        try:
            result = ingest_file(str(tmp), sheet_name="NonExistentSheet")
            assert result.success is False
            assert "NonExistentSheet" in result.error
        finally:
            tmp.unlink(missing_ok=True)

    def test_output_filename_xlsx(self):
        rows = [{"x": 1}]
        tmp = _make_temp_csv(rows, suffix=".xlsx")
        try:
            result = ingest_file(str(tmp))
            assert result.dataset_meta.output_filename.endswith(".xlsx")
        finally:
            tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Mode B — SQLite (no external DB required)
# ---------------------------------------------------------------------------


class TestIngestDbTable:
    ROWS = [
        {"vendor_id": f"V-{i:04d}", "vendor_name": f"Vendor {i}", "amount": i * 500.0}
        for i in range(1, 11)
    ]

    def test_happy_path_table(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS, table="vendors")
        try:
            result = ingest_db_table(conn_str, "vendors")
            assert result.success is True
            assert result.dataframe is not None
            assert len(result.dataframe) == 10
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_happy_path_custom_query(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS, table="vendors")
        try:
            result = ingest_db_table(conn_str, "SELECT * FROM vendors WHERE amount > 2500")
            assert result.success is True
            assert len(result.dataframe) == 5  # rows 6-10 have amount > 2500
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_source_type_is_database(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS)
        try:
            result = ingest_db_table(conn_str, "vendors")
            assert result.dataset_meta.source_type == SourceType.DATABASE
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_source_table_stored_in_meta(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS, table="invoices")
        try:
            result = ingest_db_table(conn_str, "invoices")
            assert result.dataset_meta.source_table == "invoices"
            assert result.dataset_meta.source_query is None
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_custom_query_stored_in_meta(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS)
        query = "SELECT * FROM vendors"
        try:
            result = ingest_db_table(conn_str, query)
            assert result.dataset_meta.source_query == query
            assert result.dataset_meta.source_table is None
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_output_table_name_derived(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS, table="vendor_master")
        try:
            result = ingest_db_table(conn_str, "vendor_master")
            tbl = result.dataset_meta.output_table
            assert tbl is not None
            assert tbl.startswith("vendor_master_clean_")
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_credentials_not_in_meta(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS)
        try:
            result = ingest_db_table(conn_str, "vendors")
            meta = result.dataset_meta
            # DatasetMeta should not expose a connection string field
            assert not hasattr(meta, "connection_string")
            # Dialect parsed, but no password/username fields
            assert not hasattr(meta, "db_password")
            assert not hasattr(meta, "db_username")
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_dialect_extracted_for_sqlite(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS)
        try:
            result = ingest_db_table(conn_str, "vendors")
            assert result.dataset_meta.db_dialect == "sqlite"
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_invalid_table_name_rejected(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS)
        try:
            result = ingest_db_table(conn_str, "vendors; DROP TABLE vendors;")
            assert result.success is False
            assert "Invalid table name" in result.error
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_schema_dot_table_allowed(self):
        """schema.table notation should pass the identifier check."""
        from backend.tools.ingest import _is_safe_identifier
        assert _is_safe_identifier("public.vendors") is True

    def test_row_limit_enforced(self, monkeypatch):
        monkeypatch.setattr("backend.tools.ingest.MAX_RECORDS", 5)
        conn_str, db_path = _make_sqlite_db(self.ROWS)  # 10 rows
        try:
            result = ingest_db_table(conn_str, "vendors")
            assert result.success is False
            assert "exceeds the maximum" in result.error
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_bad_connection_string_returns_failure(self):
        result = ingest_db_table("postgresql://bad:creds@localhost:9999/ghost", "vendors")
        assert result.success is False
        assert result.error is not None

    def test_credentials_scrubbed_from_error(self):
        # Use a connection string with obvious credentials
        conn_str = "postgresql+psycopg2://secretuser:secretpass@localhost:9999/mydb"
        result = ingest_db_table(conn_str, "vendors")
        assert result.success is False
        # Password must not appear in the surfaced error
        assert "secretpass" not in result.error
        assert "secretuser" not in result.error

    def test_column_meta_populated_from_db(self):
        conn_str, db_path = _make_sqlite_db(self.ROWS)
        try:
            result = ingest_db_table(conn_str, "vendors")
            assert len(result.dataset_meta.columns) == 3
            names = result.dataset_meta.column_names()
            assert "vendor_id" in names
            assert "amount" in names
        finally:
            Path(db_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Shared — helper unit tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_is_safe_identifier_valid(self):
        from backend.tools.ingest import _is_safe_identifier

        assert _is_safe_identifier("vendors") is True
        assert _is_safe_identifier("vendor_master") is True
        assert _is_safe_identifier("schema.table") is True
        assert _is_safe_identifier("Table123") is True

    def test_is_safe_identifier_invalid(self):
        from backend.tools.ingest import _is_safe_identifier

        assert _is_safe_identifier("vendors; DROP TABLE") is False
        assert _is_safe_identifier("vendor-master") is False
        assert _is_safe_identifier("vendor master") is False
        assert _is_safe_identifier("") is False

    def test_parse_provenance_extracts_fields(self):
        from backend.tools.ingest import _parse_connection_provenance

        p = _parse_connection_provenance("postgresql+psycopg2://user:pass@db.host.com:5432/mydb")
        assert p["dialect"] == "postgresql"
        assert p["host"] == "db.host.com"
        assert p["database"] == "mydb"

    def test_parse_provenance_no_credentials(self):
        from backend.tools.ingest import _parse_connection_provenance

        p = _parse_connection_provenance("postgresql+psycopg2://user:pass@host/db")
        assert "user" not in str(p)
        assert "pass" not in str(p)

    def test_scrub_credentials(self):
        from backend.tools.ingest import _scrub_credentials

        conn = "postgresql://myuser:mypassword@host/db"
        error = "Connection refused for myuser with password mypassword"
        scrubbed = _scrub_credentials(error, conn)
        assert "mypassword" not in scrubbed
        assert "myuser" not in scrubbed
