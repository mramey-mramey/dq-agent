"""
Dataset metadata model.

Tracks everything tied to a single ingest run: source type, file/DB provenance,
column schema, row counts, and output target. This record is created at ingest
and referenced by issues, audit log entries, and export operations throughout
the session.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    FILE = "FILE"  # Mode A — CSV or Excel upload
    DATABASE = "DATABASE"  # Mode B — live DB connection


class FileFormat(str, Enum):
    CSV = "csv"
    XLSX = "xlsx"
    XLS = "xls"


class ColumnMeta(BaseModel):
    """Inferred metadata for a single column."""

    name: str
    dtype: str  # pandas dtype string, e.g. "object", "int64", "float64", "datetime64[ns]"
    nullable: bool  # True if any nulls were observed at ingest
    unique_count: int  # Approximate cardinality
    sample_values: list[Any] = Field(default_factory=list)  # Up to 5 representative values


class DatasetMeta(BaseModel):
    """
    Immutable record created at ingest time.
    All downstream operations reference dataset_id.
    """

    dataset_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_type: SourceType
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # --- Mode A fields ---
    original_filename: str | None = None  # e.g. "vendors_q3.csv"
    file_format: FileFormat | None = None  # Derived from extension
    temp_file_path: str | None = None  # Absolute path to temp copy on disk
    sheet_name: str | None = None  # Excel only; None for CSV

    # --- Mode B fields ---
    # Connection string is NOT stored here — it lives in session state only.
    # We record enough provenance to describe the source without persisting credentials.
    db_dialect: str | None = None  # e.g. "postgresql", "mysql", "sqlite"
    db_host: str | None = None  # Host only, no credentials
    db_name: str | None = None  # Database name
    source_table: str | None = None  # Table name, or None if custom query
    source_query: str | None = None  # Custom SELECT query, if provided

    # --- Shared fields ---
    row_count: int = 0
    column_count: int = 0
    columns: list[ColumnMeta] = Field(default_factory=list)

    # Output target — populated at ingest, confirmed before export
    output_filename: str | None = None  # Mode A: e.g. "vendors_q3_clean_20240315T143000.csv"
    output_table: str | None = None  # Mode B: e.g. "vendors_clean_20240315T143000"

    # Runtime flag — set to True once all approved cleanses have been executed
    export_ready: bool = False

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column_by_name(self, name: str) -> ColumnMeta | None:
        return next((c for c in self.columns if c.name == name), None)
