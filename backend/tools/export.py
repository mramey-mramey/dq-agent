"""
backend/tools/export.py

Export layer for the DQ agent. Writes the cleansed DataFrame to its final
output destination once all approved cleanses have been executed.

Two modes — destination is determined by DatasetMeta.source_type:

  Mode A — File (CSV / Excel)
      export_clean_file(df, meta, ...)  -> ExportResult
      Writes to OUTPUT_DIR/{output_filename}.
      Returns a download path the Streamlit UI can serve.

  Mode B — Database table
      export_clean_table(df, meta, connection_string, ...) -> ExportResult
      Writes to a new table in the source schema.
      Never touches the original source table.

Design constraints (from CLAUDE.md):
  - Source is NEVER overwritten. File export always writes a new filename;
    DB export always writes a new table (if_exists="fail" by default).
  - Credentials are NOT stored — connection_string is used once to open the
    write connection and is never persisted or logged.
  - export_ready guard: export is blocked if meta.export_ready is already True
    (prevents double-export of the same dataset run).
  - DatasetMeta.export_ready is set to True on success.
  - Every export attempt (success or failure) writes a closing AuditEntry.
  - Column order and dtypes from the clean DataFrame are preserved as-is;
    no additional transformation is performed here.
"""

from __future__ import annotations

import logging
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field

from backend.models.dataset import DatasetMeta, FileFormat, SourceType
from backend.tools.cleanse import AuditEntry, AuditLog

logger = logging.getLogger(__name__)

# Re-use OUTPUT_DIR from ingest config
import os
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/tmp/dq_outputs"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


class ExportResult(BaseModel):
    """Result of a single export call."""

    model_config = {"arbitrary_types_allowed": True}

    success: bool
    dataset_id: str
    mode: str = ""                   # "FILE" or "DATABASE"
    output_path: str | None = None   # Mode A: absolute path to written file
    output_table: str | None = None  # Mode B: table name written
    row_count: int = 0
    column_count: int = 0
    audit_entry: AuditEntry | None = None
    summary: str = ""
    error: str | None = None


# ---------------------------------------------------------------------------
# Mode A — File export
# ---------------------------------------------------------------------------


def export_clean_file(
    df: pd.DataFrame,
    meta: DatasetMeta,
    *,
    output_filename: str | None = None,
    output_dir: Path | None = None,
    exported_by: str = "SYSTEM",
    audit_log: AuditLog | None = None,
) -> ExportResult:
    """
    Write the clean DataFrame to a new CSV or Excel file.

    Args:
        df:               The cleansed DataFrame to export.
        meta:             DatasetMeta for this dataset run. Must have
                          source_type == FILE and a valid file_format.
        output_filename:  Override the filename derived at ingest time.
                          If None, uses meta.output_filename.
        output_dir:       Override the output directory.
                          If None, uses OUTPUT_DIR from environment.
        exported_by:      Username written to the audit log.
        audit_log:        If provided, closing AuditEntry is appended here.

    Returns:
        ExportResult with output_path set on success.
    """
    # --- Guard: only valid for FILE source ---
    if meta.source_type != SourceType.FILE:
        return _fail(
            meta=meta,
            mode="FILE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=(
                f"export_clean_file called on dataset {meta.dataset_id} "
                f"which has source_type '{meta.source_type.value}'. "
                f"Use export_clean_table for DATABASE sources."
            ),
        )

    # --- Guard: double-export prevention ---
    if meta.export_ready:
        return _fail(
            meta=meta,
            mode="FILE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=(
                f"Dataset {meta.dataset_id} has already been exported "
                f"(export_ready=True). Create a new dataset run to re-export."
            ),
        )

    # --- Resolve output path ---
    dest_dir = output_dir or OUTPUT_DIR
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = output_filename or meta.output_filename
    if not filename:
        # Fallback: derive from dataset_id + timestamp
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        ext = _file_ext(meta.file_format)
        filename = f"dataset_{meta.dataset_id}_clean_{ts}{ext}"

    output_path = dest_dir / filename

    # --- Guard: refuse to overwrite an existing file ---
    if output_path.exists():
        return _fail(
            meta=meta,
            mode="FILE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=(
                f"Output file '{output_path}' already exists. "
                f"Choose a different filename or remove the existing file."
            ),
        )

    # --- Write ---
    try:
        file_format = meta.file_format or FileFormat.CSV
        if file_format == FileFormat.CSV:
            df.to_csv(output_path, index=False)
        else:
            # Excel — preserve sheet name from ingest if available
            sheet = meta.sheet_name or "Sheet1"
            df.to_excel(output_path, sheet_name=sheet, index=False)
    except Exception as exc:
        return _fail(
            meta=meta,
            mode="FILE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=f"Failed to write output file '{output_path}': {exc}",
        )

    # --- Mark dataset as exported ---
    meta.export_ready = True

    summary = (
        f"Exported {len(df):,} rows × {len(df.columns)} columns "
        f"to '{output_path.name}' ({file_format.value.upper()})."
    )
    logger.info("dataset_id=%s: %s", meta.dataset_id, summary)

    entry = _make_export_audit_entry(
        meta=meta,
        actor=exported_by,
        mode="FILE",
        destination=str(output_path),
        row_count=len(df),
        success=True,
        notes=summary,
    )
    if audit_log:
        audit_log.append(entry)

    return ExportResult(
        success=True,
        dataset_id=meta.dataset_id,
        mode="FILE",
        output_path=str(output_path),
        row_count=len(df),
        column_count=len(df.columns),
        audit_entry=entry,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Mode B — Database table export
# ---------------------------------------------------------------------------


def export_clean_table(
    df: pd.DataFrame,
    meta: DatasetMeta,
    connection_string: str,
    *,
    output_table: str | None = None,
    if_exists: str = "fail",
    exported_by: str = "SYSTEM",
    audit_log: AuditLog | None = None,
) -> ExportResult:
    """
    Write the clean DataFrame to a new table in the source database.

    Args:
        df:                The cleansed DataFrame to export.
        meta:              DatasetMeta for this dataset run. Must have
                           source_type == DATABASE.
        connection_string: SQLAlchemy URL for the target database.
                           Used once to open the write connection; never stored.
        output_table:      Override the table name derived at ingest time.
                           If None, uses meta.output_table.
        if_exists:         Behaviour if the output table already exists.
                           "fail" (default) | "replace".
                           "append" is intentionally NOT supported — appending
                           to an existing clean table could silently corrupt it.
        exported_by:       Username written to the audit log.
        audit_log:         If provided, closing AuditEntry is appended here.

    Returns:
        ExportResult with output_table set on success.
    """
    # --- Guard: only valid for DATABASE source ---
    if meta.source_type != SourceType.DATABASE:
        return _fail(
            meta=meta,
            mode="DATABASE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=(
                f"export_clean_table called on dataset {meta.dataset_id} "
                f"which has source_type '{meta.source_type.value}'. "
                f"Use export_clean_file for FILE sources."
            ),
        )

    # --- Guard: double-export prevention ---
    if meta.export_ready:
        return _fail(
            meta=meta,
            mode="DATABASE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=(
                f"Dataset {meta.dataset_id} has already been exported "
                f"(export_ready=True). Create a new dataset run to re-export."
            ),
        )

    # --- Validate if_exists ---
    if if_exists not in ("fail", "replace"):
        return _fail(
            meta=meta,
            mode="DATABASE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=(
                f"Invalid if_exists value '{if_exists}'. "
                f"Only 'fail' and 'replace' are supported. "
                f"'append' is not supported to protect clean table integrity."
            ),
        )

    # --- Resolve output table name ---
    table_name = output_table or meta.output_table
    if not table_name:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        source = meta.source_table or "query"
        table_name = f"{source}_clean_{ts}"

    # --- Validate table name (SQL injection guard) ---
    if not _is_safe_identifier(table_name):
        return _fail(
            meta=meta,
            mode="DATABASE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=(
                f"Invalid output table name '{table_name}'. "
                f"Only alphanumeric characters, underscores, and dots are allowed."
            ),
        )

    # --- Write ---
    try:
        from sqlalchemy import create_engine

        engine = create_engine(connection_string)
        with engine.begin() as conn:
            df.to_sql(
                table_name,
                conn,
                if_exists=if_exists,
                index=False,
                method="multi",
            )
        engine.dispose()

    except ImportError:
        return _fail(
            meta=meta,
            mode="DATABASE",
            exported_by=exported_by,
            audit_log=audit_log,
            error="SQLAlchemy is not installed. Run: pip install sqlalchemy",
        )
    except Exception as exc:
        safe_error = _scrub_credentials(str(exc), connection_string)
        return _fail(
            meta=meta,
            mode="DATABASE",
            exported_by=exported_by,
            audit_log=audit_log,
            error=f"Database write failed: {safe_error}",
        )

    # --- Mark dataset as exported ---
    meta.export_ready = True
    # Store the resolved table name back so callers can retrieve it
    meta.output_table = table_name

    # Parse provenance for the summary (no credentials)
    prov = _parse_connection_provenance(connection_string)
    destination = (
        f"{prov['dialect']}://{prov['host']}/{prov['database']}.{table_name}"
    )

    summary = (
        f"Exported {len(df):,} rows × {len(df.columns)} columns "
        f"to table '{table_name}' on {prov['dialect']}://{prov['host']}."
    )
    logger.info("dataset_id=%s: %s", meta.dataset_id, summary)

    entry = _make_export_audit_entry(
        meta=meta,
        actor=exported_by,
        mode="DATABASE",
        destination=destination,
        row_count=len(df),
        success=True,
        notes=summary,
    )
    if audit_log:
        audit_log.append(entry)

    return ExportResult(
        success=True,
        dataset_id=meta.dataset_id,
        mode="DATABASE",
        output_table=table_name,
        row_count=len(df),
        column_count=len(df.columns),
        audit_entry=entry,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fail(
    meta: DatasetMeta,
    mode: str,
    exported_by: str,
    audit_log: AuditLog | None,
    error: str,
) -> ExportResult:
    """Construct a failed ExportResult and write an audit entry."""
    logger.error("Export failed for dataset %s: %s", meta.dataset_id, error)
    entry = _make_export_audit_entry(
        meta=meta,
        actor=exported_by,
        mode=mode,
        destination="",
        row_count=0,
        success=False,
        notes=error,
    )
    if audit_log:
        audit_log.append(entry)
    return ExportResult(
        success=False,
        dataset_id=meta.dataset_id,
        mode=mode,
        audit_entry=entry,
        error=error,
    )


def _make_export_audit_entry(
    meta: DatasetMeta,
    actor: str,
    mode: str,
    destination: str,
    row_count: int,
    success: bool,
    notes: str,
) -> AuditEntry:
    return AuditEntry(
        dataset_id=meta.dataset_id,
        issue_id="EXPORT",          # Sentinel value — export has no issue_id
        action_type=f"EXPORT_{mode}",
        actor=actor,
        affected_columns=[],
        affected_row_indices=[],
        before_values={},
        after_values={
            "destination": destination,
            "row_count": row_count,
        },
        success=success,
        notes=notes,
    )


def _file_ext(file_format: FileFormat | None) -> str:
    if file_format in (FileFormat.XLSX, FileFormat.XLS):
        return ".xlsx"
    return ".csv"


def _is_safe_identifier(name: str) -> bool:
    import re
    return bool(re.match(r"^[A-Za-z0-9_.]+$", name))


def _parse_connection_provenance(connection_string: str) -> dict[str, str]:
    try:
        parsed = urllib.parse.urlparse(connection_string)
        dialect = parsed.scheme.split("+")[0] if parsed.scheme else "unknown"
        host = parsed.hostname or "unknown"
        database = parsed.path.lstrip("/") if parsed.path else ""
        return {"dialect": dialect, "host": host, "database": database}
    except Exception:
        return {"dialect": "unknown", "host": "unknown", "database": ""}


def _scrub_credentials(error_msg: str, connection_string: str) -> str:
    try:
        parsed = urllib.parse.urlparse(connection_string)
        if parsed.password:
            error_msg = error_msg.replace(parsed.password, "***")
        if parsed.username:
            error_msg = error_msg.replace(parsed.username, "***")
    except Exception:
        pass
    return error_msg