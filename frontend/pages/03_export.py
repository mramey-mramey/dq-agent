"""
frontend/pages/03_export.py

Step 03 — Export Clean Dataset

Routes to the correct export path based on session source_type:
  Mode A (FILE)     → export_clean_file → download link
  Mode B (DATABASE) → export_clean_table → confirmation with table name

Shows a pre-export checklist: unresolved HIGH/CRITICAL issues,
remaining APPROVED issues not yet executed.
"""

import streamlit as st

from components.ui import (
    DL_AMBER,
    DL_BLUE,
    DL_DARK_GRAY,
    DL_GRAY,
    DL_GREEN,
    DL_RED,
    DL_WHITE,
    APIError,
    api_execute,
    api_export_file,
    api_export_table,
    api_list_issues,
    api_download_file,
    inject_brand_css,
    init_session_state,
    render_page_header,
    render_section_header,
    require_session,
    sidebar_session_info,
)

st.set_page_config(page_title="Export · DQ Agent", page_icon="📤", layout="wide")
inject_brand_css()
init_session_state()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div style="font-size:1rem;font-weight:700;color:white;padding-bottom:6px;">'
        'Data Quality Agent</div>',
        unsafe_allow_html=True,
    )
    st.page_link("app.py",                label="🏠 Home")
    st.page_link("pages/01_ingest.py",    label="📥 01 · Ingest")
    st.page_link("pages/02_review.py",    label="🔎 02 · Review")
    st.page_link("pages/03_export.py",    label="📤 03 · Export")
    st.page_link("pages/04_scorecard.py", label="📊 04 · Scorecard")
    st.page_link("pages/05_audit_log.py", label="📋 05 · Audit Log")
    sidebar_session_info()

render_page_header(
    "📤 Export Clean Dataset",
    "Write the cleansed dataset to a new file or database table. "
    "The original source is never overwritten.",
)

if not require_session():
    st.stop()

sid       = st.session_state["session_id"]
username  = st.session_state.get("username", "analyst")
source    = st.session_state.get("source_type", "FILE")

# ── Pre-export checklist ──────────────────────────────────────────────────────
render_section_header("Pre-export Checklist")

try:
    all_issues = api_list_issues(sid)
except APIError as e:
    st.error(f"Could not load issues: {e.detail}")
    all_issues = []

open_high = [i for i in all_issues
             if i["status"] in ("OPEN", "PENDING_APPROVAL")
             and i["severity"] in ("HIGH", "CRITICAL")
             and i.get("is_actionable", False)]

approved_pending = [i for i in all_issues if i["status"] == "APPROVED"]
executed = [i for i in all_issues if i["status"] == "EXECUTED"]
rejected = [i for i in all_issues if i["status"] == "REJECTED"]

check_col1, check_col2, check_col3, check_col4 = st.columns(4)
items = [
    ("Unresolved HIGH/CRITICAL", len(open_high), DL_RED if open_high else DL_GREEN,
     "🟢" if not open_high else "🔴"),
    ("Approved, awaiting execution", len(approved_pending),
     DL_AMBER if approved_pending else DL_GREEN,
     "⏳" if approved_pending else "🟢"),
    ("Fixes executed", len(executed), DL_BLUE, "⚡"),
    ("Rejected (no change)", len(rejected), DL_DARK_GRAY, "✗"),
]
for col, (label, count, color, icon) in zip(
    [check_col1, check_col2, check_col3, check_col4], items
):
    with col:
        st.markdown(
            f'<div style="background:{DL_WHITE};border:1px solid {DL_GRAY};'
            f'border-top:3px solid {color};border-radius:4px;padding:14px;text-align:center;">'
            f'<div style="font-size:1.5rem;">{icon}</div>'
            f'<div style="font-size:1.8rem;font-weight:700;color:{color};">{count}</div>'
            f'<div style="font-size:0.72rem;color:#555;text-transform:uppercase;'
            f'letter-spacing:0.06em;margin-top:4px;">{label}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

# ── Execute approved (if any pending) ────────────────────────────────────────
if approved_pending:
    st.markdown("<br>", unsafe_allow_html=True)
    st.warning(
        f"⚠️ {len(approved_pending)} approved fix(es) have not been executed yet. "
        "Execute them now before exporting, or export the current state.",
        icon="⚠️",
    )
    if st.button("⚡ Execute All Approved Now", key="exec_before_export"):
        with st.spinner("Executing…"):
            try:
                res = api_execute(sid, username)
                st.success(
                    f"Executed {res['succeeded']} fix(es). "
                    f"{res['failed']} failed. {res['skipped']} skipped."
                )
                st.rerun()
            except APIError as e:
                st.error(f"Execute failed: {e.detail}")

# ── Warn on unresolved critical/high ─────────────────────────────────────────
if open_high:
    st.markdown("<br>", unsafe_allow_html=True)
    st.error(
        f"🔴 {len(open_high)} actionable HIGH or CRITICAL issue(s) are unresolved. "
        "You can still export, but the output may contain significant data quality problems.",
        icon="🔴",
    )

# ── Already exported ─────────────────────────────────────────────────────────
if st.session_state.get("export_done"):
    st.markdown("<br>", unsafe_allow_html=True)
    st.success("✅ This dataset has already been exported.")
    if source == "FILE" and st.session_state.get("export_path"):
        import os
        filename = os.path.basename(st.session_state["export_path"])
        try:
            file_bytes = api_download_file(filename)
            st.download_button(
                label=f"⬇️  Download {filename}",
                data=file_bytes,
                file_name=filename,
                mime="application/octet-stream",
                key="redownload_btn",
            )
        except APIError:
            st.info(f"Output file: `{filename}`")
    elif source == "DATABASE" and st.session_state.get("export_table"):
        st.info(f"Exported to table: `{st.session_state['export_table']}`")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# MODE A — File export
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)

if source == "FILE":
    render_section_header("📁 Export to File")
    left, right = st.columns([2, 1], gap="large")

    with left:
        output_filename = st.text_input(
            "Output filename (optional override)",
            key="export_filename_override",
            placeholder="vendors_clean_20240315T143000.csv — leave blank to use default",
        ) or None

        st.caption(
            "The file will be written to the server's output directory. "
            "A download link will appear immediately after export."
        )

        if st.button("📤 Export Clean File", key="export_file_btn"):
            with st.spinner("Writing clean file…"):
                try:
                    result = api_export_file(sid, output_filename=output_filename)
                    st.session_state["export_done"] = True
                    st.session_state["export_path"] = result["output_path"]
                    st.success(f"✅ {result['summary']}")

                    import os
                    filename = os.path.basename(result["output_path"])
                    try:
                        file_bytes = api_download_file(filename)
                        st.download_button(
                            label=f"⬇️  Download {filename}",
                            data=file_bytes,
                            file_name=filename,
                            mime="application/octet-stream",
                            key="download_btn",
                        )
                    except APIError:
                        st.info(f"File written to: `{result['output_path']}`")
                except APIError as e:
                    st.error(f"Export failed: {e.detail}")

    with right:
        st.markdown(
            f"""
            <div style="background:{DL_GREEN}10;border:1px solid {DL_GREEN}40;
                        border-radius:4px;padding:14px 16px;font-size:0.82rem;
                        color:{DL_DARK_GRAY};line-height:1.6;">
                <b>What gets exported?</b><br>
                The working DataFrame after all executed fixes are applied.
                Column order and data types are preserved.<br><br>
                <b>Format</b><br>
                Same as the original upload (CSV stays CSV, Excel stays Excel).<br><br>
                <b>Original file</b><br>
                Never overwritten. The output is always a new, timestamped file.
            </div>
            """,
            unsafe_allow_html=True,
        )

# ─────────────────────────────────────────────────────────────────────────────
# MODE B — Database table export
# ─────────────────────────────────────────────────────────────────────────────
else:
    render_section_header("🗄️ Export to Database Table")
    left, right = st.columns([2, 1], gap="large")

    with left:
        output_table = st.text_input(
            "Output table name (optional override)",
            key="export_table_override",
            placeholder="vendors_clean_20240315T143000 — leave blank for default",
        ) or None

        if_exists = st.radio(
            "If the output table already exists:",
            options=["fail", "replace"],
            horizontal=True,
            key="if_exists_radio",
            help="'fail' is the safe default. 'replace' will drop and recreate the table.",
        )

        st.caption(
            "🔒 The connection string from your ingest session is used. "
            "The original source table is never modified."
        )

        if st.button("📤 Export to Database", key="export_table_btn"):
            with st.spinner("Writing to database…"):
                try:
                    result = api_export_table(
                        sid,
                        output_table_name=output_table,
                        if_exists=if_exists,
                    )
                    st.session_state["export_done"] = True
                    st.session_state["export_table"] = result["output_table"]
                    st.success(
                        f"✅ {result['summary']}\n\n"
                        f"Output table: `{result['output_table']}`"
                    )
                except APIError as e:
                    st.error(f"Export failed: {e.detail}")

    with right:
        st.markdown(
            f"""
            <div style="background:{DL_GREEN}10;border:1px solid {DL_GREEN}40;
                        border-radius:4px;padding:14px 16px;font-size:0.82rem;
                        color:{DL_DARK_GRAY};line-height:1.6;">
                <b>What gets exported?</b><br>
                All rows from the clean working DataFrame are written to a
                new table in the same database and schema as the source.<br><br>
                <b>Source table</b><br>
                Never modified. Output goes to a separate table.<br><br>
                <b>if_exists = fail</b><br>
                Default. Returns an error if the table already exists,
                protecting against accidental overwrites.
            </div>
            """,
            unsafe_allow_html=True,
        )
