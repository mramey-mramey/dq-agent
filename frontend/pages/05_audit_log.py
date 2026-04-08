"""
frontend/pages/05_audit_log.py

Step 05 — Audit Log

Displays the immutable, append-only audit trail for the session:
  • Every approval, rejection, execution, and export
  • Actor, timestamp, before/after values where relevant
  • Filterable by action type and outcome
  • Export to CSV for compliance handoff

The audit log supports SOX and federal audit requirements (CLAUDE.md design #5).
"""

import io
import csv
from datetime import datetime

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
    api_audit_log,
    inject_brand_css,
    init_session_state,
    render_page_header,
    render_section_header,
    require_session,
    sidebar_session_info,
)

st.set_page_config(page_title="Audit Log · DQ Agent", page_icon="📋", layout="wide")
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
    "📋 Audit Log",
    "Immutable record of every decision made during this session. "
    "All entries are append-only and cannot be modified.",
)

if not require_session():
    st.stop()

sid = st.session_state["session_id"]

# ── Load entries ──────────────────────────────────────────────────────────────
if st.button("🔄 Refresh", key="refresh_audit"):
    st.cache_data.clear()

@st.cache_data(ttl=5, show_spinner=False)
def _load_audit(_sid: str) -> list[dict]:
    try:
        return api_audit_log(_sid)
    except APIError as e:
        st.error(f"Could not load audit log: {e.detail}")
        return []

entries = _load_audit(sid)

if not entries:
    st.info(
        "No audit entries yet. Approvals, rejections, executions, and exports "
        "will appear here as they happen."
    )
    st.stop()

# ── Summary strip ─────────────────────────────────────────────────────────────
total        = len(entries)
successes    = sum(1 for e in entries if e["success"])
failures     = total - successes
unique_actors = len({e["actor"] for e in entries})
action_types  = sorted({e["action_type"] for e in entries})

sum_col1, sum_col2, sum_col3, sum_col4 = st.columns(4)
summary_items = [
    ("Total Entries",   str(total),          DL_DARK_GRAY),
    ("Successful",      str(successes),      DL_GREEN),
    ("Failures",        str(failures),       DL_RED if failures else DL_DARK_GRAY),
    ("Unique Actors",   str(unique_actors),  DL_BLUE),
]
for col, (label, value, color) in zip(
    [sum_col1, sum_col2, sum_col3, sum_col4], summary_items
):
    with col:
        st.markdown(
            f'<div style="background:{DL_WHITE};border:1px solid {DL_GRAY};'
            f'border-top:3px solid {color};border-radius:4px;'
            f'padding:12px 14px;text-align:center;">'
            f'<div style="font-size:1.8rem;font-weight:700;color:{color};">{value}</div>'
            f'<div style="font-size:0.7rem;color:#555;text-transform:uppercase;'
            f'letter-spacing:0.06em;margin-top:4px;">{label}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

st.markdown("<br>", unsafe_allow_html=True)

# ── Filters ───────────────────────────────────────────────────────────────────
fc1, fc2, fc3 = st.columns(3)
with fc1:
    filter_action = st.multiselect(
        "Action type",
        options=action_types,
        default=[],
        key="audit_action_filter",
    )
with fc2:
    filter_outcome = st.radio(
        "Outcome",
        options=["All", "Success only", "Failures only"],
        horizontal=True,
        key="audit_outcome_filter",
    )
with fc3:
    filter_actor = st.selectbox(
        "Actor",
        options=["All"] + sorted({e["actor"] for e in entries}),
        key="audit_actor_filter",
    )

# Apply filters
def _filter(entries: list[dict]) -> list[dict]:
    result = entries
    if filter_action:
        result = [e for e in result if e["action_type"] in filter_action]
    if filter_outcome == "Success only":
        result = [e for e in result if e["success"]]
    elif filter_outcome == "Failures only":
        result = [e for e in result if not e["success"]]
    if filter_actor != "All":
        result = [e for e in result if e["actor"] == filter_actor]
    return result

filtered = _filter(entries)

# ── CSV export ────────────────────────────────────────────────────────────────
def _to_csv(rows: list[dict]) -> bytes:
    if not rows:
        return b""
    buf = io.StringIO()
    fields = ["entry_id", "timestamp", "issue_id", "action_type",
              "actor", "success", "affected_row_count", "notes"]
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode()


st.markdown("<br>", unsafe_allow_html=True)
header_col, export_col = st.columns([4, 1])
with header_col:
    render_section_header(f"Audit Entries ({len(filtered)} of {total})")
with export_col:
    st.download_button(
        label="⬇️ Export CSV",
        data=_to_csv(filtered),
        file_name=f"audit_log_{sid[:8]}.csv",
        mime="text/csv",
        key="audit_csv_btn",
    )

# ── Entry table ───────────────────────────────────────────────────────────────
if not filtered:
    st.info("No entries match the current filters.")
    st.stop()

# Table header
st.markdown(
    f"""
    <table style="width:100%;border-collapse:collapse;
                  font-family:'Open Sans',sans-serif;font-size:0.8rem;">
        <thead>
            <tr style="background:{DL_DARK_GRAY};color:white;">
                <th style="padding:9px 12px;text-align:left;font-size:0.7rem;
                           text-transform:uppercase;letter-spacing:0.06em;
                           white-space:nowrap;">Timestamp (UTC)</th>
                <th style="padding:9px 12px;text-align:left;font-size:0.7rem;
                           text-transform:uppercase;letter-spacing:0.06em;">Issue ID</th>
                <th style="padding:9px 12px;text-align:left;font-size:0.7rem;
                           text-transform:uppercase;letter-spacing:0.06em;">Action</th>
                <th style="padding:9px 12px;text-align:left;font-size:0.7rem;
                           text-transform:uppercase;letter-spacing:0.06em;">Actor</th>
                <th style="padding:9px 12px;text-align:center;font-size:0.7rem;
                           text-transform:uppercase;letter-spacing:0.06em;">Outcome</th>
                <th style="padding:9px 12px;text-align:center;font-size:0.7rem;
                           text-transform:uppercase;letter-spacing:0.06em;">Rows</th>
                <th style="padding:9px 12px;text-align:left;font-size:0.7rem;
                           text-transform:uppercase;letter-spacing:0.06em;">Notes</th>
            </tr>
        </thead>
    </table>
    """,
    unsafe_allow_html=True,
)

# Render rows — alternating stripe
for idx, entry in enumerate(filtered):
    bg = DL_WHITE if idx % 2 == 0 else "#F9F9F9"
    ts_raw = entry.get("timestamp", "")
    try:
        ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        ts = ts_raw[:19] if ts_raw else "—"

    success = entry.get("success", False)
    outcome_html = (
        f'<span style="color:{DL_GREEN};font-weight:700;">✓ OK</span>'
        if success else
        f'<span style="color:{DL_RED};font-weight:700;">✗ Fail</span>'
    )

    action = entry.get("action_type", "")
    action_color = {
        "EXPORT_FILE": DL_BLUE,
        "EXPORT_DATABASE": DL_BLUE,
    }.get(action, DL_DARK_GRAY)

    issue_id = entry.get("issue_id", "")
    actor    = entry.get("actor", "")
    rows_aff = entry.get("affected_row_count", 0)
    notes    = entry.get("notes", "")
    # Truncate long notes
    notes_display = (notes[:60] + "…") if len(notes) > 60 else notes

    st.markdown(
        f"""
        <div style="background:{bg};border-bottom:1px solid {DL_GRAY};
                    padding:8px 12px;display:grid;
                    grid-template-columns:160px 130px 180px 160px 80px 60px 1fr;
                    gap:8px;align-items:center;font-size:0.8rem;">
            <div style="color:#555;white-space:nowrap;">{ts}</div>
            <div style="font-family:monospace;font-size:0.73rem;color:{DL_DARK_GRAY};">
                {issue_id[:14] if issue_id else "—"}</div>
            <div style="font-weight:600;color:{action_color};">{action}</div>
            <div style="color:{DL_DARK_GRAY};">{actor}</div>
            <div style="text-align:center;">{outcome_html}</div>
            <div style="text-align:center;color:#555;">{rows_aff}</div>
            <div style="color:#666;font-size:0.75rem;white-space:nowrap;
                        overflow:hidden;text-overflow:ellipsis;"
                 title="{notes}">{notes_display}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ── Compliance note ───────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    f"""
    <div style="background:{DL_GREEN}08;border:1px solid {DL_GREEN}30;
                border-radius:4px;padding:12px 16px;
                font-size:0.78rem;color:#555;">
        <b>Compliance note:</b> This audit log is append-only during the session.
        All entries — approvals, rejections, executions, and exports — are recorded
        with the actor's username and timestamp. Export the CSV above for inclusion
        in compliance documentation or SOX review packages.
    </div>
    """,
    unsafe_allow_html=True,
)
