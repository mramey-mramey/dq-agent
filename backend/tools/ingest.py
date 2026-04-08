"""
backend/tools/ingest.py

Ingestion layer for the DQ agent. Handles two modes:

  Mode A — File upload (CSV / Excel)
      ingest_file(file_path, sheet_name) -> IngestResult

  Mode B — Live database connection
      ingest_db_table(connection_string, table_or_query) -> IngestResult

Both modes return an IngestResult containing:
  - A populated DatasetMeta record
  - The loaded pandas DataFrame (held in memory for the session)
  - A human-readable summary string for the agent

Design constraints (from CLAUDE.md):
  - Source data is NEVER modified — file uploads are copied to a temp path;
    DB connections are opened read-only.
  - Credentials are NOT logged or persisted — the connection_string parameter
    is used once to open the connection and is not stored in DatasetMeta.
  - Row limit enforced via DQ_MAX_RECORDS_PER_RUN (default 50,000).
  - Column type inference is stored in DatasetMeta.columns for downstream rules.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel

from backend.models.dataset import ColumnMeta, DatasetMeta, FileFormat, SourceType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAX_RECORDS = int(os.getenv("DQ_MAX_RECORDS_PER_RUN", "50000"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/tmp/dq_outputs"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SUPPORTED_FILE_EXTENSIONS = {".csv", ".xlsx", ".xls"}

# Number of sample values to capture per column for DQ context
SAMPLE_VALUE_COUNT = 5


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


class IngestResult(BaseModel):
    """
    Returned by both ingest functions.
    The DataFrame is excluded from Pydantic serialization — callers must
    store it separately (e.g., in a session-scoped dict keyed by dataset_id).
    """

    model_config = {"arbitrary_types_allowed": True}

    success: bool
    dataset_meta: DatasetMeta | None = None
    dataframe: pd.DataFrame | None = None  # Not serialized — session memory only
    summary: str = ""  # Plain-language summary for the agent / UI
    error: str | None = None  # Set on failure


# ---------------------------------------------------------------------------
# Mode A — File Upload
# ---------------------------------------------------------------------------


def ingest_file(
    file_path: str,
    sheet_name: str | None = None,
) -> IngestResult:
    """
    Read a CSV or Excel file into a pandas DataFrame.

    Args:
        file_path:   Absolute path to the uploaded file on disk.
        sheet_name:  Excel only. Defaults to the first sheet if None.

    Returns:
        IngestResult with populated DatasetMeta and DataFrame.
    """
    path = Path(file_path)

    # --- Validate file exists ---
    if not path.exists():
        return IngestResult(success=False, error=f"File not found: {file_path}")

    ext = path.suffix.lower()
    if ext not in SUPPORTED_FILE_EXTENSIONS:
        return IngestResult(
            success=False,
            error=f"Unsupported file type '{ext}'. Supported: {SUPPORTED_FILE_EXTENSIONS}",
        )

    # --- Copy to temp location so original is never touched ---
    try:
        tmp_dir = tempfile.mkdtemp(prefix="dq_ingest_")
        tmp_path = Path(tmp_dir) / path.name
        shutil.copy2(path, tmp_path)
        logger.info("Copied '%s' → temp '%s'", path, tmp_path)
    except OSError as e:
        return IngestResult(success=False, error=f"Failed to copy file to temp: {e}")

    # --- Read into DataFrame ---
    try:
        if ext == ".csv":
            df = pd.read_csv(tmp_path, dtype_backend="numpy_nullable")
            resolved_sheet = None
            file_format = FileFormat.CSV
        else:
            # Excel: resolve sheet name
            xl = pd.ExcelFile(tmp_path)
            if sheet_name is None:
                resolved_sheet = xl.sheet_names[0]
            elif sheet_name not in xl.sheet_names:
                return IngestResult(
                    success=False,
                    error=(
                        f"Sheet '{sheet_name}' not found. "
                        f"Available sheets: {xl.sheet_names}"
                    ),
                )
            else:
                resolved_sheet = sheet_name

            df = pd.read_excel(tmp_path, sheet_name=resolved_sheet, dtype_backend="numpy_nullable")
            file_format = FileFormat.XLSX if ext == ".xlsx" else FileFormat.XLS

    except Exception as e:
        return IngestResult(success=False, error=f"Failed to parse file: {e}")

    # --- Enforce row limit ---
    row_limit_result = _check_row_limit(df, source_label=path.name)
    if row_limit_result:
        return row_limit_result

    # --- Build column metadata ---
    columns = _build_column_meta(df)

    # --- Derive output filename ---
    timestamp = _now_timestamp()
    stem = path.stem
    output_filename = f"{stem}_clean_{timestamp}{_output_ext(file_format)}"

    # --- Assemble DatasetMeta ---
    meta = DatasetMeta(
        source_type=SourceType.FILE,
        original_filename=path.name,
        file_format=file_format,
        temp_file_path=str(tmp_path),
        sheet_name=resolved_sheet,
        row_count=len(df),
        column_count=len(df.columns),
        columns=columns,
        output_filename=output_filename,
    )

    summary = (
        f"Ingested file '{path.name}' "
        f"({'sheet: ' + resolved_sheet if resolved_sheet else 'CSV'}). "
        f"{meta.row_count:,} rows × {meta.column_count} columns. "
        f"dataset_id: {meta.dataset_id}. "
        f"Output will be written to '{output_filename}'."
    )
    logger.info(summary)

    return IngestResult(success=True, dataset_meta=meta, dataframe=df, summary=summary)


# ---------------------------------------------------------------------------
# Mode B — Live Database Connection
# ---------------------------------------------------------------------------


def ingest_db_table(
    connection_string: str,
    table_or_query: str,
) -> IngestResult:
    """
    Read a database table or query result into a pandas DataFrame.

    The connection is opened read-only (SELECT only). Credentials are used
    once to open the connection and are NOT stored in DatasetMeta.

    Args:
        connection_string:  SQLAlchemy connection URL, e.g.
                            "postgresql+psycopg2://user:pass@host:5432/mydb"
                            Credentials are extracted for host/db provenance
                            only and are not persisted.
        table_or_query:     Table name (e.g. "vendors") or a full SELECT query.

    Returns:
        IngestResult with populated DatasetMeta and DataFrame.
    """
    # --- Parse dialect / host / db for provenance (no credentials stored) ---
    provenance = _parse_connection_provenance(connection_string)

    # --- Build the SQL expression ---
    is_query = table_or_query.strip().upper().startswith("SELECT")
    if is_query:
        sql = table_or_query
        source_table = None
        source_query = table_or_query
    else:
        # Treat as table name — validate it's safe before interpolating
        table_name = table_or_query.strip()
        if not _is_safe_identifier(table_name):
            return IngestResult(
                success=False,
                error=(
                    f"Invalid table name '{table_name}'. "
                    "Use only alphanumeric characters, underscores, and dots."
                ),
            )
        sql = f"SELECT * FROM {table_name}"
        source_table = table_name
        source_query = None

    # --- Connect and read ---
    try:
        # Import here so SQLAlchemy is only required when Mode B is used
        from sqlalchemy import create_engine, text

        engine = create_engine(connection_string, connect_args={"options": "-c default_transaction_read_only=on"} if "postgresql" in connection_string else {})

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

        engine.dispose()

    except ImportError:
        return IngestResult(
            success=False,
            error="SQLAlchemy is not installed. Run: pip install sqlalchemy",
        )
    except Exception as e:
        # Scrub the connection string from the error message before surfacing
        safe_error = _scrub_credentials(str(e), connection_string)
        return IngestResult(success=False, error=f"Database read failed: {safe_error}")

    # --- Enforce row limit ---
    label = source_table or "query result"
    row_limit_result = _check_row_limit(df, source_label=label)
    if row_limit_result:
        return row_limit_result

    # --- Build column metadata ---
    columns = _build_column_meta(df)

    # --- Derive output table name ---
    timestamp = _now_timestamp()
    base = source_table or "query"
    output_table = f"{base}_clean_{timestamp}"

    # --- Assemble DatasetMeta (no credentials stored) ---
    meta = DatasetMeta(
        source_type=SourceType.DATABASE,
        db_dialect=provenance.get("dialect"),
        db_host=provenance.get("host"),
        db_name=provenance.get("database"),
        source_table=source_table,
        source_query=source_query,
        row_count=len(df),
        column_count=len(df.columns),
        columns=columns,
        output_table=output_table,
    )

    source_desc = f"table '{source_table}'" if source_table else "custom query"
    summary = (
        f"Ingested {source_desc} from "
        f"{provenance.get('dialect', 'database')}://{provenance.get('host', 'unknown')}"
        f"/{provenance.get('database', '')}. "
        f"{meta.row_count:,} rows × {meta.column_count} columns. "
        f"dataset_id: {meta.dataset_id}. "
        f"Output will be written to table '{output_table}'."
    )
    logger.info(summary)

    return IngestResult(success=True, dataset_meta=meta, dataframe=df, summary=summary)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _build_column_meta(df: pd.DataFrame) -> list[ColumnMeta]:
    """Infer ColumnMeta for each column in the DataFrame."""
    columns = []
    for col in df.columns:
        series = df[col]
        nullable = bool(series.isna().any())
        unique_count = int(series.nunique(dropna=True))

        # Sample up to SAMPLE_VALUE_COUNT non-null values
        non_null = series.dropna()
        sample_raw = non_null.head(SAMPLE_VALUE_COUNT).tolist()
        # Convert numpy/pandas scalars to plain Python for JSON serialisation
        sample_values = [_to_python_scalar(v) for v in sample_raw]

        columns.append(
            ColumnMeta(
                name=str(col),
                dtype=str(series.dtype),
                nullable=nullable,
                unique_count=unique_count,
                sample_values=sample_values,
            )
        )
    return columns


def _check_row_limit(df: pd.DataFrame, source_label: str) -> IngestResult | None:
    """Return an error IngestResult if the DataFrame exceeds MAX_RECORDS, else None."""
    if len(df) > MAX_RECORDS:
        return IngestResult(
            success=False,
            error=(
                f"'{source_label}' contains {len(df):,} rows, which exceeds the "
                f"maximum of {MAX_RECORDS:,} (DQ_MAX_RECORDS_PER_RUN). "
                "Reduce the dataset size or narrow the query and try again."
            ),
        )
    return None


def _parse_connection_provenance(connection_string: str) -> dict[str, str]:
    """
    Extract dialect, host, and database name from a connection string
    WITHOUT retaining any credentials.
    """
    try:
        parsed = urllib.parse.urlparse(connection_string)
        dialect = parsed.scheme.split("+")[0] if parsed.scheme else "unknown"
        host = parsed.hostname or "unknown"
        database = parsed.path.lstrip("/") if parsed.path else ""
        return {"dialect": dialect, "host": host, "database": database}
    except Exception:
        return {"dialect": "unknown", "host": "unknown", "database": ""}


def _scrub_credentials(error_msg: str, connection_string: str) -> str:
    """
    Remove any credentials embedded in the connection string from an error
    message before it is surfaced to the user or logged.
    """
    try:
        parsed = urllib.parse.urlparse(connection_string)
        if parsed.password:
            error_msg = error_msg.replace(parsed.password, "***")
        if parsed.username:
            error_msg = error_msg.replace(parsed.username, "***")
    except Exception:
        pass
    return error_msg


def _is_safe_identifier(name: str) -> bool:
    """
    Validate that a table name contains only safe characters to prevent
    SQL injection when interpolating into a SELECT statement.
    Allows: letters, digits, underscores, dots (for schema.table notation).
    """
    return bool(re.match(r"^[A-Za-z0-9_.]+$", name))


def _now_timestamp() -> str:
    """Return a compact UTC timestamp string suitable for filenames/table names."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _output_ext(file_format: FileFormat) -> str:
    return ".xlsx" if file_format in (FileFormat.XLSX, FileFormat.XLS) else ".csv"


def _to_python_scalar(value: Any) -> Any:
    """Convert numpy/pandas scalar types to plain Python for JSON safety."""
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
    if isinstance(value, float) and (value != value):  # NaN check
        return None
    return value
