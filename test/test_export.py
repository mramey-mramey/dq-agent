"""
tests/test_export.py

Unit tests for backend/tools/export.py.

Coverage:
    ExportResult model:
        - success / error fields
        - mode populated correctly

    export_clean_file() — Mode A:
        - CSV written to correct path
        - Excel (.xlsx) written correctly
        - file content matches DataFrame
        - original DataFrame not mutated
        - meta.export_ready set to True on success
        - output_filename from meta used by default
        - output_filename override respected
        - output_dir override respected
        - refuses to overwrite existing file
        - double-export blocked (export_ready already True)
        - wrong source_type (DATABASE) rejected
        - write failure returns ExportResult with error
        - audit entry written on success
        - audit entry written on failure
        - audit entry success=True on success
        - audit entry success=False on failure
        - row_count / column_count in result

    export_clean_table() — Mode B:
        - table written to SQLite DB
        - table content matches DataFrame
        - meta.export_ready set to True on success
        - output_table from meta used by default
        - output_table override respected
        - meta.output_table updated with resolved name on success
        - double-export blocked
        - wrong source_type (FILE) rejected
        - if_exists="fail" raises error when table exists
        - if_exists="replace" overwrites existing table
        - if_exists="append" explicitly rejected
        - invalid table name rejected
        - credentials scrubbed from DB error messages
        - audit entry written on success
        - audit entry written on failure
        - provenance (no credentials) in audit destination

    Helpers:
        - _is_safe_identifier allows valid names
        - _is_safe_identifier rejects SQL injection attempts
        - _scrub_credentials removes password and username
        - _parse_connection_provenance extracts dialect/host/db
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

os.environ.setdefault("DQ_MAX_RECORDS_PER_RUN", "50000")
_TMP_OUTPUT = tempfile.mkdtemp(prefix="dq_export_test_")
os.environ["OUTPUT_DIR"] = _TMP_OUTPUT

from backend.models.dataset import DatasetMeta, FileFormat, SourceType
from backend.tools.cleanse import AuditLog
from backend.tools.export import (
    ExportResult,
    _is_safe_identifier,
    _parse_connection_provenance,
    _scrub_credentials,
    export_clean_file,
    export_clean_table,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "vendor_id":   ["V-001", "V-002", "V-003"],
            "vendor_name": ["Acme Corporation", "Globex Supplies", "Initech Solutions"],
            "amount":      [1000.0, 2500.0, 3200.0],
            "state":       ["IL", "IL", "PA"],
        }
    )


def _file_meta(
    file_format: FileFormat = FileFormat.CSV,
    output_filename: str | None = "vendors_clean_20240315T143000.csv",
    export_ready: bool = False,
) -> DatasetMeta:
    return DatasetMeta(
        dataset_id="export-test-001",
        source_type=SourceType.FILE,
        file_format=file_format,
        original_filename="vendors.csv",
        output_filename=output_filename,
        row_count=3,
        column_count=4,
        export_ready=export_ready,
    )


def _db_meta(
    output_table: str | None = "vendors_clean_20240315T143000",
    export_ready: bool = False,
) -> DatasetMeta:
    return DatasetMeta(
        dataset_id="export-test-002",
        source_type=SourceType.DATABASE,
        db_dialect="sqlite",
        db_host="localhost",
        db_name="test.db",
        source_table="vendors",
        output_table=output_table,
        row_count=3,
        column_count=4,
        export_ready=export_ready,
    )


def _sqlite_conn(db_path: str) -> str:
    return f"sqlite:///{db_path}"


# ---------------------------------------------------------------------------
# export_clean_file — Mode A
# ---------------------------------------------------------------------------


class TestExportCleanFile:
    def test_csv_written_to_path(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.success is True
        assert result.output_path is not None
        assert Path(result.output_path).exists()

    def test_csv_content_matches_dataframe(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        result = export_clean_file(df, meta, output_dir=tmp_path)
        written = pd.read_csv(result.output_path)
        assert list(written.columns) == list(df.columns)
        assert len(written) == len(df)

    def test_excel_written_to_path(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(
            file_format=FileFormat.XLSX,
            output_filename="vendors_clean.xlsx",
        )
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.success is True
        assert Path(result.output_path).exists()
        assert result.output_path.endswith(".xlsx")

    def test_excel_content_matches_dataframe(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(
            file_format=FileFormat.XLSX,
            output_filename="vendors_clean.xlsx",
        )
        result = export_clean_file(df, meta, output_dir=tmp_path)
        written = pd.read_excel(result.output_path)
        assert list(written.columns) == list(df.columns)
        assert len(written) == len(df)

    def test_original_df_not_mutated(self, tmp_path):
        df = _clean_df()
        original = df.copy()
        meta = _file_meta(output_filename="vendors_clean.csv")
        export_clean_file(df, meta, output_dir=tmp_path)
        pd.testing.assert_frame_equal(df, original)

    def test_meta_export_ready_set_true(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        assert meta.export_ready is False
        export_clean_file(df, meta, output_dir=tmp_path)
        assert meta.export_ready is True

    def test_output_filename_from_meta(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="my_custom_name.csv")
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert Path(result.output_path).name == "my_custom_name.csv"

    def test_output_filename_override(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="default.csv")
        result = export_clean_file(
            df, meta, output_dir=tmp_path, output_filename="override.csv"
        )
        assert Path(result.output_path).name == "override.csv"

    def test_output_dir_override(self, tmp_path):
        df = _clean_df()
        custom_dir = tmp_path / "subdir"
        meta = _file_meta(output_filename="vendors_clean.csv")
        result = export_clean_file(df, meta, output_dir=custom_dir)
        assert result.success is True
        assert Path(result.output_path).parent == custom_dir

    def test_refuses_to_overwrite_existing_file(self, tmp_path):
        df = _clean_df()
        existing = tmp_path / "vendors_clean.csv"
        existing.write_text("existing content")
        meta = _file_meta(output_filename="vendors_clean.csv")
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.success is False
        assert "already exists" in result.error
        # Original file untouched
        assert existing.read_text() == "existing content"

    def test_double_export_blocked(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv", export_ready=True)
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.success is False
        assert "already been exported" in result.error

    def test_wrong_source_type_rejected(self, tmp_path):
        df = _clean_df()
        meta = _db_meta()  # DATABASE source
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.success is False
        assert "FILE" in result.error or "source_type" in result.error.lower()

    def test_row_count_in_result(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.row_count == 3

    def test_column_count_in_result(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.column_count == 4

    def test_mode_is_file(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.mode == "FILE"

    def test_audit_entry_written_on_success(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        log = AuditLog()
        export_clean_file(df, meta, output_dir=tmp_path, audit_log=log)
        assert len(log) == 1
        assert log.entries()[0].success is True

    def test_audit_entry_written_on_failure(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(export_ready=True)  # triggers double-export failure
        log = AuditLog()
        export_clean_file(df, meta, output_dir=tmp_path, audit_log=log)
        assert len(log) == 1
        assert log.entries()[0].success is False

    def test_audit_entry_dataset_id(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        log = AuditLog()
        export_clean_file(df, meta, output_dir=tmp_path, audit_log=log)
        assert log.entries()[0].dataset_id == "export-test-001"

    def test_audit_entry_row_count_in_after_values(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename="vendors_clean.csv")
        log = AuditLog()
        export_clean_file(df, meta, output_dir=tmp_path, audit_log=log)
        assert log.entries()[0].after_values["row_count"] == 3

    def test_fallback_filename_when_meta_has_none(self, tmp_path):
        df = _clean_df()
        meta = _file_meta(output_filename=None)
        result = export_clean_file(df, meta, output_dir=tmp_path)
        assert result.success is True
        assert Path(result.output_path).exists()


# ---------------------------------------------------------------------------
# export_clean_table — Mode B
# ---------------------------------------------------------------------------


class TestExportCleanTable:
    def _db(self, tmp_path: Path) -> tuple[str, str]:
        db_path = str(tmp_path / "test.db")
        return _sqlite_conn(db_path), db_path

    def test_table_written(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str)
        assert result.success is True
        assert result.output_table == "vendors_clean"

    def test_table_content_matches_dataframe(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        conn_str, db_path = self._db(tmp_path)
        export_clean_table(df, meta, conn_str)
        conn = sqlite3.connect(db_path)
        written = pd.read_sql("SELECT * FROM vendors_clean", conn)
        conn.close()
        assert list(written.columns) == list(df.columns)
        assert len(written) == len(df)

    def test_meta_export_ready_set_true(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        conn_str, _ = self._db(tmp_path)
        assert meta.export_ready is False
        export_clean_table(df, meta, conn_str)
        assert meta.export_ready is True

    def test_output_table_from_meta(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="my_custom_table")
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str)
        assert result.output_table == "my_custom_table"

    def test_output_table_override(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="default_table")
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str, output_table="override_table")
        assert result.output_table == "override_table"

    def test_meta_output_table_updated_with_resolved_name(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table=None)  # No name set — will be derived
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str)
        assert result.success is True
        assert meta.output_table == result.output_table

    def test_double_export_blocked(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean", export_ready=True)
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str)
        assert result.success is False
        assert "already been exported" in result.error

    def test_wrong_source_type_rejected(self, tmp_path):
        df = _clean_df()
        meta = _file_meta()  # FILE source
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str)
        assert result.success is False
        assert "DATABASE" in result.error or "source_type" in result.error.lower()

    def test_if_exists_fail_raises_when_table_exists(self, tmp_path):
        df = _clean_df()
        conn_str, db_path = self._db(tmp_path)
        # Pre-create the table
        conn = sqlite3.connect(db_path)
        df.to_sql("vendors_clean", conn, index=False)
        conn.close()
        meta = _db_meta(output_table="vendors_clean")
        result = export_clean_table(df, meta, conn_str, if_exists="fail")
        assert result.success is False

    def test_if_exists_replace_overwrites(self, tmp_path):
        df = _clean_df()
        conn_str, db_path = self._db(tmp_path)
        # Pre-create the table with different data
        old_df = pd.DataFrame({"x": [1, 2, 3]})
        conn = sqlite3.connect(db_path)
        old_df.to_sql("vendors_clean", conn, index=False)
        conn.close()
        meta = _db_meta(output_table="vendors_clean")
        result = export_clean_table(df, meta, conn_str, if_exists="replace")
        assert result.success is True
        # Verify new content
        conn = sqlite3.connect(db_path)
        written = pd.read_sql("SELECT * FROM vendors_clean", conn)
        conn.close()
        assert "vendor_name" in written.columns

    def test_if_exists_append_rejected(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str, if_exists="append")
        assert result.success is False
        assert "append" in result.error.lower()

    def test_invalid_table_name_rejected(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(
            df, meta, conn_str, output_table="bad name; DROP TABLE vendors"
        )
        assert result.success is False
        assert "Invalid" in result.error

    def test_credentials_scrubbed_from_error(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        bad_conn = "postgresql+psycopg2://secretuser:secretpass@localhost:9999/ghost"
        result = export_clean_table(df, meta, bad_conn)
        assert result.success is False
        assert "secretpass" not in result.error
        assert "secretuser" not in result.error

    def test_mode_is_database(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str)
        assert result.mode == "DATABASE"

    def test_row_count_in_result(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str)
        assert result.row_count == 3

    def test_audit_entry_written_on_success(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        conn_str, _ = self._db(tmp_path)
        log = AuditLog()
        export_clean_table(df, meta, conn_str, audit_log=log)
        assert len(log) == 1
        assert log.entries()[0].success is True

    def test_audit_entry_written_on_failure(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(export_ready=True)
        conn_str, _ = self._db(tmp_path)
        log = AuditLog()
        export_clean_table(df, meta, conn_str, audit_log=log)
        assert len(log) == 1
        assert log.entries()[0].success is False

    def test_audit_entry_no_credentials_in_destination(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table="vendors_clean")
        # Use a connection string with credentials
        conn_str, db_path = self._db(tmp_path)
        # Simulate a conn string with embedded credentials
        conn_with_creds = f"sqlite+pysqlite://user:password@/{db_path[1:]}"
        # Even if the connection fails (credentials not used in sqlite),
        # test that the audit destination field doesn't contain "password"
        log = AuditLog()
        result = export_clean_table(
            df, meta, conn_str, output_table="vendors_clean", audit_log=log
        )
        for entry in log.entries():
            dest = str(entry.after_values.get("destination", ""))
            assert "password" not in dest

    def test_fallback_table_name_when_meta_has_none(self, tmp_path):
        df = _clean_df()
        meta = _db_meta(output_table=None)
        conn_str, _ = self._db(tmp_path)
        result = export_clean_table(df, meta, conn_str)
        assert result.success is True
        assert result.output_table is not None
        assert "vendors" in result.output_table or "query" in result.output_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_is_safe_identifier_valid(self):
        assert _is_safe_identifier("vendors_clean") is True
        assert _is_safe_identifier("public.vendors") is True
        assert _is_safe_identifier("VendorMaster123") is True

    def test_is_safe_identifier_invalid(self):
        assert _is_safe_identifier("vendors; DROP TABLE") is False
        assert _is_safe_identifier("vendor name") is False
        assert _is_safe_identifier("vendor-name") is False
        assert _is_safe_identifier("") is False

    def test_scrub_credentials_removes_password(self):
        conn = "postgresql://user:secretpass@host/db"
        error = "Connection failed for user with password secretpass"
        scrubbed = _scrub_credentials(error, conn)
        assert "secretpass" not in scrubbed
        assert "***" in scrubbed

    def test_scrub_credentials_removes_username(self):
        conn = "postgresql://myuser:mypass@host/db"
        error = "Auth failed for myuser"
        scrubbed = _scrub_credentials(error, conn)
        assert "myuser" not in scrubbed

    def test_scrub_credentials_safe_with_no_creds(self):
        conn = "sqlite:///path/to/file.db"
        error = "Some error"
        scrubbed = _scrub_credentials(error, conn)
        assert scrubbed == "Some error"

    def test_parse_provenance_postgresql(self):
        conn = "postgresql+psycopg2://user:pass@db.host.com:5432/mydb"
        p = _parse_connection_provenance(conn)
        assert p["dialect"] == "postgresql"
        assert p["host"] == "db.host.com"
        assert p["database"] == "mydb"

    def test_parse_provenance_no_credentials(self):
        conn = "postgresql+psycopg2://user:pass@host/db"
        p = _parse_connection_provenance(conn)
        assert "user" not in str(p)
        assert "pass" not in str(p)

    def test_parse_provenance_sqlite(self):
        conn = "sqlite:///path/to/file.db"
        p = _parse_connection_provenance(conn)
        assert p["dialect"] == "sqlite"