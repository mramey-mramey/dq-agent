"""
frontend/pages/01_ingest.py

Step 01 — Ingest

Two modes:
  Mode A  Upload a CSV or Excel file
  Mode B  Connect to a live database and select a table or query

After ingest, the page runs the agent to perform quality checks and surfaces
an inline chat for follow-up questions.
"""

import streamlit as st

from components.ui import (
    DL_DARK_GRAY,
    DL_GREEN,
    inject_brand_css,
    init_session_state,
    render_page_header,
    render_section_header,
    sidebar_session_info,
    api_chat,
    api_create_session,
    api_ingest_db,
    api_ingest_file,
    APIError,
)

st.set_page_config(page_title="Ingest · DQ Agent", page_icon="📥", layout="wide")
inject_brand_css()
init_session_state()

# ── Sidebar nav ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        f'<div style="font-size:1rem;font-weight:700;color:white;padding-bottom:6px;">'
        f'Data Quality Agent</div>',
        unsafe_allow_html=True,
    )
    st.page_link("app.py",                label="🏠 Home")
    st.page_link("pages/01_ingest.py",    label="📥 01 · Ingest")
    st.page_link("pages/02_review.py",    label="🔎 02 · Review")
    st.page_link("pages/03_export.py",    label="📤 03 · Export")
    st.page_link("pages/04_scorecard.py", label="📊 04 · Scorecard")
    st.page_link("pages/05_audit_log.py", label="📋 05 · Audit Log")
    sidebar_session_info()

# ── Page header ───────────────────────────────────────────────────────────────
render_page_header(
    "📥 Ingest Dataset",
    "Upload a CSV/Excel file or connect to a live database. "
    "The agent will run quality checks automatically.",
)

# ── Mode selector ─────────────────────────────────────────────────────────────
mode = st.radio(
    "Data source",
    options=["📁 File Upload (CSV / Excel)", "🗄️ Live Database Connection"],
    horizontal=True,
    key="ingest_mode",
    label_visibility="collapsed",
)
file_mode = mode.startswith("📁")

st.markdown("<br>", unsafe_allow_html=True)

# ── Helper: create or reuse session ──────────────────────────────────────────
def _ensure_session() -> str:
    if not st.session_state.get("session_id"):
        with st.spinner("Creating session…"):
            try:
                sid = api_create_session()
                st.session_state["session_id"] = sid
            except APIError as e:
                st.error(f"Could not create session: {e.detail}")
                st.stop()
    return st.session_state["session_id"]


def _run_initial_checks(session_id: str) -> None:
    """Ask the agent to run quality checks and cache the result."""
    with st.spinner("🔍 Running quality checks…"):
        try:
            result = api_chat(
                session_id=session_id,
                message=(
                    f"I've ingested dataset {st.session_state['dataset_id']}. "
                    "Please run all quality checks and give me a plain-English summary "
                    "of the top findings, then ask what I'd like to do next."
                ),
                history=[],
            )
            st.session_state["conversation"] = result["new_history"]
        except APIError as e:
            st.warning(f"Quality checks could not run automatically: {e.detail}")


# ─────────────────────────────────────────────────────────────────────────────
# MODE A — File upload
# ─────────────────────────────────────────────────────────────────────────────
if file_mode:
    left, right = st.columns([2, 1], gap="large")

    with left:
        render_section_header("Upload File")
        uploaded = st.file_uploader(
            "Drop your CSV or Excel file here",
            type=["csv", "xlsx", "xls"],
            key="file_uploader",
            help="Maximum 50,000 rows. Larger datasets should be pre-filtered.",
        )

        sheet_name: str | None = None
        if uploaded and uploaded.name.endswith((".xlsx", ".xls")):
            sheet_name = st.text_input(
                "Sheet name (leave blank for first sheet)",
                key="sheet_name_input",
                placeholder="Sheet1",
            ) or None

        if uploaded:
            st.markdown(
                f'<div style="font-size:0.8rem;color:#555;margin-top:4px;">'
                f'<b>{uploaded.name}</b> · {uploaded.size / 1024:.1f} KB</div>',
                unsafe_allow_html=True,
            )

        ingest_clicked = st.button(
            "Ingest & Run Quality Checks",
            key="ingest_file_btn",
            disabled=uploaded is None,
        )

        if ingest_clicked and uploaded:
            sid = _ensure_session()
            with st.spinner(f"Ingesting {uploaded.name}…"):
                try:
                    result = api_ingest_file(
                        session_id=sid,
                        file_bytes=uploaded.read(),
                        filename=uploaded.name,
                        sheet_name=sheet_name,
                    )
                    st.session_state["dataset_id"] = result["dataset_id"]
                    st.session_state["ingest_summary"] = result["summary"]
                    st.session_state["source_type"] = "FILE"
                    st.session_state["issues"] = []
                    st.session_state["export_done"] = False
                    st.session_state["conversation"] = []
                    st.success(f"✅ Ingested **{result['row_count']:,}** rows × **{result['column_count']}** columns.")
                except APIError as e:
                    st.error(f"Ingest failed: {e.detail}")
                    st.stop()

            _run_initial_checks(sid)
            st.rerun()

    with right:
        render_section_header("Format Guide")
        st.markdown(
            f"""
            <div style="background:{DL_GREEN}10;border:1px solid {DL_GREEN}40;
                        border-radius:4px;padding:14px 16px;font-size:0.82rem;
                        color:{DL_DARK_GRAY};line-height:1.6;">
                <b>Supported formats</b><br>
                CSV · Excel (.xlsx / .xls)<br><br>
                <b>Best practices</b><br>
                • First row should be column headers<br>
                • Include a unique ID column (e.g. vendor_id)<br>
                • Numeric amounts as plain numbers (no $ signs)<br>
                • Dates as YYYY-MM-DD where possible<br><br>
                <b>Limit</b><br>
                50,000 rows per run
            </div>
            """,
            unsafe_allow_html=True,
        )

# ─────────────────────────────────────────────────────────────────────────────
# MODE B — Database connection
# ─────────────────────────────────────────────────────────────────────────────
else:
    left, right = st.columns([2, 1], gap="large")

    with left:
        render_section_header("Database Connection")
        st.info(
            "🔒 Your connection string is held in session memory only — "
            "it is never logged, stored, or echoed in any response.",
            icon="🔒",
        )

        conn_str = st.text_input(
            "Connection string",
            key="db_conn_str",
            placeholder="postgresql+psycopg2://user:pass@host:5432/mydb",
            type="password",
            help="SQLAlchemy URL. Supported: PostgreSQL, MySQL, MSSQL, SQLite.",
        )
        table_or_query = st.text_area(
            "Table name or SELECT query",
            key="db_table_query",
            placeholder="vendors\n\n— or —\n\nSELECT * FROM vendors WHERE active = 1",
            height=100,
        )

        ingest_db_clicked = st.button(
            "Connect & Run Quality Checks",
            key="ingest_db_btn",
            disabled=not (conn_str and table_or_query),
        )

        if ingest_db_clicked and conn_str and table_or_query:
            sid = _ensure_session()
            with st.spinner("Connecting and reading data…"):
                try:
                    result = api_ingest_db(
                        session_id=sid,
                        connection_string=conn_str,
                        table_or_query=table_or_query.strip(),
                    )
                    st.session_state["dataset_id"] = result["dataset_id"]
                    st.session_state["ingest_summary"] = result["summary"]
                    st.session_state["source_type"] = "DATABASE"
                    st.session_state["issues"] = []
                    st.session_state["export_done"] = False
                    st.session_state["conversation"] = []
                    st.success(f"✅ Connected. Loaded **{result['row_count']:,}** rows × **{result['column_count']}** columns.")
                except APIError as e:
                    st.error(f"Connection failed: {e.detail}")
                    st.stop()

            _run_initial_checks(sid)
            st.rerun()

    with right:
        render_section_header("Supported Drivers")
        st.markdown(
            f"""
            <div style="background:{DL_GREEN}10;border:1px solid {DL_GREEN}40;
                        border-radius:4px;padding:14px 16px;font-size:0.82rem;
                        color:{DL_DARK_GRAY};line-height:1.7;">
                <b>PostgreSQL</b><br>
                <code style="font-size:0.72rem;">postgresql+psycopg2://…</code><br><br>
                <b>MySQL / MariaDB</b><br>
                <code style="font-size:0.72rem;">mysql+pymysql://…</code><br><br>
                <b>SQL Server</b><br>
                <code style="font-size:0.72rem;">mssql+pyodbc://…</code><br><br>
                <b>SQLite</b><br>
                <code style="font-size:0.72rem;">sqlite:///path/to/file.db</code>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ─────────────────────────────────────────────────────────────────────────────
# Agent chat panel (shown after successful ingest)
# ─────────────────────────────────────────────────────────────────────────────
if st.session_state.get("dataset_id"):
    st.markdown("<br>", unsafe_allow_html=True)
    render_section_header("🤖 Agent")

    # Display conversation history
    conversation = st.session_state.get("conversation", [])
    for msg in conversation:
        role = msg.get("role", "")
        content = msg.get("content", "")
        if isinstance(content, list):
            # Extract text blocks only (skip tool_use / tool_result)
            text = " ".join(
                b.get("text", "") if isinstance(b, dict) else getattr(b, "text", "")
                for b in content
                if (isinstance(b, dict) and b.get("type") == "text")
                or (hasattr(b, "type") and b.type == "text")
            ).strip()
            if not text:
                continue
            content = text
        if role == "user" and not (content or "").strip():
            continue
        with st.chat_message(role if role in ("user", "assistant") else "assistant"):
            st.markdown(content)

    # Chat input
    user_input = st.chat_input("Ask the agent about your data quality issues…")
    if user_input:
        sid = st.session_state["session_id"]
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                try:
                    result = api_chat(
                        session_id=sid,
                        message=user_input,
                        history=st.session_state["conversation"],
                    )
                    st.session_state["conversation"] = (
                        st.session_state["conversation"] + result["new_history"]
                    )
                    st.markdown(result["text"])
                except APIError as e:
                    st.error(f"Agent error: {e.detail}")

        st.rerun()
