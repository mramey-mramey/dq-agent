"""
frontend/components/ui.py

Shared UI utilities for the DQ Agent Streamlit frontend.

Contains:
  - Deloitte brand CSS injection (inject_brand_css)
  - Severity / status color helpers
  - Reusable metric card renderer
  - Session state initialisation
  - API client wrapper around the FastAPI backend
"""

from __future__ import annotations

import os
from typing import Any

import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
DEFAULT_ROLE = os.getenv("DEFAULT_ROLE", "analyst")
DEFAULT_USER = os.getenv("DEFAULT_USER", "analyst@deloitte.com")


# ---------------------------------------------------------------------------
# Brand constants
# ---------------------------------------------------------------------------

DL_GREEN       = "#86BC25"
DL_NEON_GREEN  = "#86EB22"
DL_BLUE        = "#00A3E0"
DL_DARK_GRAY   = "#282728"
DL_GRAY        = "#E6E6E6"
DL_AMBER       = "#E8A317"
DL_RED         = "#DA291C"
DL_WHITE       = "#FFFFFF"

SEVERITY_COLORS = {
    "CRITICAL": DL_RED,
    "HIGH":     DL_AMBER,
    "MEDIUM":   DL_BLUE,
    "LOW":      DL_GREEN,
}

SEVERITY_ICONS = {
    "CRITICAL": "🔴",
    "HIGH":     "🟠",
    "MEDIUM":   "🔵",
    "LOW":      "🟢",
}

STATUS_COLORS = {
    "OPEN":             "#888888",
    "PENDING_APPROVAL": DL_AMBER,
    "APPROVED":         DL_GREEN,
    "REJECTED":         DL_RED,
    "EXECUTED":         DL_BLUE,
}

STATUS_ICONS = {
    "OPEN":             "○",
    "PENDING_APPROVAL": "⏳",
    "APPROVED":         "✅",
    "REJECTED":         "✗",
    "EXECUTED":         "⚡",
}


# ---------------------------------------------------------------------------
# Brand CSS
# ---------------------------------------------------------------------------

BRAND_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@300;400;600;700&display=swap');

/* ── Global reset ─────────────────────────────────────────────────────── */
html, body, [class*="css"] {{
    font-family: 'Open Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
}}

/* ── Deloitte green top bar ───────────────────────────────────────────── */
[data-testid="stAppViewContainer"] > .main {{
    padding-top: 0 !important;
}}
header[data-testid="stHeader"] {{
    border-top: 4px solid {DL_GREEN} !important;
    background: {DL_WHITE} !important;
}}

/* ── Sidebar ──────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {{
    background-color: {DL_DARK_GRAY} !important;
    border-right: 3px solid {DL_GREEN} !important;
}}
[data-testid="stSidebar"] * {{
    color: #E6E6E6 !important;
}}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stTextInput label {{
    color: #A0A0A0 !important;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}}

/* ── Metric cards ─────────────────────────────────────────────────────── */
.dl-metric-card {{
    background: {DL_WHITE};
    border: 1px solid {DL_GRAY};
    border-top: 3px solid {DL_GREEN};
    border-radius: 4px;
    padding: 16px 20px;
    margin-bottom: 8px;
}}
.dl-metric-label {{
    font-size: 0.75rem;
    font-weight: 600;
    color: #555555;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 4px;
}}
.dl-metric-value {{
    font-size: 2rem;
    font-weight: 700;
    color: {DL_DARK_GRAY};
    line-height: 1.1;
}}
.dl-metric-value.green {{ color: {DL_GREEN}; }}
.dl-metric-value.amber {{ color: {DL_AMBER}; }}
.dl-metric-value.red   {{ color: {DL_RED}; }}
.dl-metric-value.blue  {{ color: {DL_BLUE}; }}
.dl-metric-sub {{
    font-size: 0.75rem;
    color: #555555;
    margin-top: 4px;
}}

/* ── Issue cards ──────────────────────────────────────────────────────── */
.dl-issue-card {{
    background: {DL_WHITE};
    border: 1px solid {DL_GRAY};
    border-left: 4px solid {DL_GRAY};
    border-radius: 4px;
    padding: 14px 16px;
    margin-bottom: 10px;
}}
.dl-issue-card.CRITICAL {{ border-left-color: {DL_RED}; }}
.dl-issue-card.HIGH     {{ border-left-color: {DL_AMBER}; }}
.dl-issue-card.MEDIUM   {{ border-left-color: {DL_BLUE}; }}
.dl-issue-card.LOW      {{ border-left-color: {DL_GREEN}; }}

.dl-badge {{
    display: inline-block;
    font-size: 0.68rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 3px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-right: 4px;
    color: {DL_WHITE};
}}
.dl-badge-sev-CRITICAL {{ background: {DL_RED}; }}
.dl-badge-sev-HIGH     {{ background: {DL_AMBER}; color: {DL_DARK_GRAY}; }}
.dl-badge-sev-MEDIUM   {{ background: {DL_BLUE}; }}
.dl-badge-sev-LOW      {{ background: {DL_GREEN}; }}
.dl-badge-status-OPEN             {{ background: #888888; }}
.dl-badge-status-PENDING_APPROVAL {{ background: {DL_AMBER}; color: {DL_DARK_GRAY}; }}
.dl-badge-status-APPROVED         {{ background: {DL_GREEN}; }}
.dl-badge-status-REJECTED         {{ background: {DL_RED}; }}
.dl-badge-status-EXECUTED         {{ background: {DL_BLUE}; }}

/* ── Section headers ──────────────────────────────────────────────────── */
.dl-section-header {{
    border-bottom: 2px solid {DL_GREEN};
    padding-bottom: 6px;
    margin-bottom: 16px;
    font-size: 1.1rem;
    font-weight: 700;
    color: {DL_DARK_GRAY};
}}

/* ── Confidence bar ───────────────────────────────────────────────────── */
.dl-conf-bar-wrap {{
    background: {DL_GRAY};
    border-radius: 3px;
    height: 6px;
    width: 100%;
    overflow: hidden;
}}
.dl-conf-bar-fill {{
    height: 6px;
    border-radius: 3px;
    background: linear-gradient(90deg, {DL_GREEN}, {DL_NEON_GREEN});
}}

/* ── Score gauge ──────────────────────────────────────────────────────── */
.dl-score-ring {{
    text-align: center;
    padding: 16px;
}}
.dl-score-number {{
    font-size: 3.5rem;
    font-weight: 700;
    line-height: 1;
}}

/* ── Page title ───────────────────────────────────────────────────────── */
.dl-page-title {{
    font-size: 1.6rem;
    font-weight: 700;
    color: {DL_DARK_GRAY};
    margin-bottom: 4px;
}}
.dl-page-sub {{
    font-size: 0.9rem;
    color: #555555;
    margin-bottom: 20px;
    border-bottom: 1px solid {DL_GRAY};
    padding-bottom: 12px;
}}

/* ── Audit table ──────────────────────────────────────────────────────── */
.dl-audit-row {{
    padding: 10px 0;
    border-bottom: 1px solid {DL_GRAY};
    font-size: 0.85rem;
}}
.dl-audit-success {{ color: {DL_GREEN}; font-weight: 600; }}
.dl-audit-fail    {{ color: {DL_RED};   font-weight: 600; }}

/* ── Buttons ──────────────────────────────────────────────────────────── */
.stButton > button {{
    background-color: {DL_GREEN} !important;
    color: {DL_WHITE} !important;
    border: none !important;
    font-weight: 600 !important;
    border-radius: 3px !important;
    padding: 6px 18px !important;
}}
.stButton > button:hover {{
    background-color: #75A521 !important;
}}

/* ── Streamlit default overrides ──────────────────────────────────────── */
.stProgress > div > div > div {{
    background-color: {DL_GREEN} !important;
}}
[data-testid="metric-container"] {{
    border-top: 3px solid {DL_GREEN};
    background: {DL_WHITE};
    padding: 12px !important;
    border-radius: 4px;
}}
</style>
"""


def inject_brand_css() -> None:
    """Inject Deloitte brand CSS into the page. Call once per page."""
    st.markdown(BRAND_CSS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------


def init_session_state() -> None:
    """Ensure all expected session state keys exist with defaults."""
    defaults = {
        "session_id":      None,    # Backend AgentSession ID
        "dataset_id":      None,    # Current dataset ID
        "ingest_summary":  None,    # Summary string from ingest
        "conversation":    [],      # Chat history list[dict]
        "issues":          [],      # Latest issues list from backend
        "source_type":     None,    # "FILE" or "DATABASE"
        "username":        DEFAULT_USER,
        "role":            DEFAULT_ROLE,
        "export_done":     False,   # True once export completed
        "export_path":     None,    # Output file path (Mode A)
        "export_table":    None,    # Output table name (Mode B)
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


class APIError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"API {status_code}: {detail}")


def _headers() -> dict[str, str]:
    return {
        "X-User-Role": st.session_state.get("role", DEFAULT_ROLE),
        "X-User-Name": st.session_state.get("username", DEFAULT_USER),
    }


def _raise_for(resp: requests.Response) -> None:
    if not resp.ok:
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        raise APIError(resp.status_code, detail)


def api_create_session() -> str:
    resp = requests.post(f"{BACKEND_URL}/sessions/")
    _raise_for(resp)
    return resp.json()["session_id"]


def api_ingest_file(session_id: str, file_bytes: bytes, filename: str, sheet_name: str | None = None) -> dict:
    files = {"file": (filename, file_bytes, "application/octet-stream")}
    data = {}
    if sheet_name:
        data["sheet_name"] = sheet_name
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/ingest/file",
        files=files,
        data=data,
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_ingest_db(session_id: str, connection_string: str, table_or_query: str) -> dict:
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/ingest/db",
        json={"connection_string": connection_string, "table_or_query": table_or_query},
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_chat(session_id: str, message: str, history: list[dict]) -> dict:
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/chat",
        json={"message": message, "history": history},
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_list_issues(session_id: str, status: str | None = None, category: str | None = None) -> list[dict]:
    params: dict = {}
    if status:
        params["status"] = status
    if category:
        params["category"] = category
    resp = requests.get(
        f"{BACKEND_URL}/sessions/{session_id}/issues",
        params=params,
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()["issues"]


def api_get_issue(session_id: str, issue_id: str) -> dict:
    resp = requests.get(
        f"{BACKEND_URL}/sessions/{session_id}/issues/{issue_id}",
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_approve_issue(session_id: str, issue_id: str, approved_by: str, note: str = "") -> dict:
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/issues/{issue_id}/approve",
        json={"approved_by": approved_by, "note": note},
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_reject_issue(session_id: str, issue_id: str, rejected_by: str, note: str = "") -> dict:
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/issues/{issue_id}/reject",
        json={"rejected_by": rejected_by, "note": note},
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_bulk_approve(session_id: str, approved_by: str, confidence_threshold: float = 0.95) -> dict:
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/approve-all",
        json={"approved_by": approved_by, "confidence_threshold": confidence_threshold},
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_execute(session_id: str, approved_by: str) -> dict:
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/execute",
        params={"approved_by": approved_by},
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_export_file(session_id: str, output_filename: str | None = None) -> dict:
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/export/file",
        json={"exported_by": st.session_state.get("username", DEFAULT_USER),
              "output_filename": output_filename},
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_export_table(session_id: str, output_table_name: str | None = None, if_exists: str = "fail") -> dict:
    resp = requests.post(
        f"{BACKEND_URL}/sessions/{session_id}/export/table",
        json={"exported_by": st.session_state.get("username", DEFAULT_USER),
              "output_table_name": output_table_name,
              "if_exists": if_exists},
        headers=_headers(),
    )
    _raise_for(resp)
    return resp.json()


def api_scorecard(session_id: str) -> dict:
    resp = requests.get(f"{BACKEND_URL}/sessions/{session_id}/scorecard", headers=_headers())
    _raise_for(resp)
    return resp.json()


def api_audit_log(session_id: str) -> list[dict]:
    resp = requests.get(f"{BACKEND_URL}/sessions/{session_id}/audit", headers=_headers())
    _raise_for(resp)
    return resp.json()["entries"]


def api_download_file(filename: str) -> bytes:
    resp = requests.get(f"{BACKEND_URL}/download/{filename}")
    _raise_for(resp)
    return resp.content


# ---------------------------------------------------------------------------
# Reusable UI components
# ---------------------------------------------------------------------------


def render_page_header(title: str, subtitle: str) -> None:
    st.markdown(f'<div class="dl-page-title">{title}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="dl-page-sub">{subtitle}</div>', unsafe_allow_html=True)


def render_section_header(text: str) -> None:
    st.markdown(f'<div class="dl-section-header">{text}</div>', unsafe_allow_html=True)


def render_metric_card(label: str, value: str, sub: str = "", color: str = "") -> None:
    color_class = f' {color}' if color else ""
    html = f"""
    <div class="dl-metric-card">
        <div class="dl-metric-label">{label}</div>
        <div class="dl-metric-value{color_class}">{value}</div>
        {"<div class='dl-metric-sub'>" + sub + "</div>" if sub else ""}
    </div>"""
    st.markdown(html, unsafe_allow_html=True)


def render_severity_badge(severity: str) -> str:
    icon = SEVERITY_ICONS.get(severity, "●")
    return f'<span class="dl-badge dl-badge-sev-{severity}">{icon} {severity}</span>'


def render_status_badge(status: str) -> str:
    icon = STATUS_ICONS.get(status, "●")
    label = status.replace("_", " ")
    return f'<span class="dl-badge dl-badge-status-{status}">{icon} {label}</span>'


def render_confidence_bar(confidence: float) -> str:
    pct = int(confidence * 100)
    color = DL_GREEN if pct >= 88 else DL_AMBER if pct >= 72 else DL_RED
    return f"""
    <div style="display:flex;align-items:center;gap:8px;">
        <div class="dl-conf-bar-wrap" style="flex:1;">
            <div class="dl-conf-bar-fill" style="width:{pct}%;background:{color};"></div>
        </div>
        <span style="font-size:0.75rem;font-weight:600;color:{DL_DARK_GRAY};min-width:36px;">{pct}%</span>
    </div>"""


def sidebar_session_info() -> None:
    """Render session/user info block in the sidebar."""
    with st.sidebar:
        st.markdown("---")
        st.markdown(
            f'<div style="font-size:0.7rem;color:#A0A0A0;text-transform:uppercase;'
            f'letter-spacing:0.08em;">Session</div>',
            unsafe_allow_html=True,
        )
        sid = st.session_state.get("session_id", "—")
        st.markdown(
            f'<div style="font-size:0.75rem;color:#E6E6E6;word-break:break-all;">'
            f'{sid[:20] + "…" if sid and len(sid) > 20 else sid}</div>',
            unsafe_allow_html=True,
        )
        if st.session_state.get("dataset_id"):
            st.markdown(
                f'<div style="font-size:0.7rem;color:#A0A0A0;margin-top:8px;'
                f'text-transform:uppercase;letter-spacing:0.08em;">Dataset</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div style="font-size:0.75rem;color:{DL_GREEN};word-break:break-all;">'
                f'{st.session_state["dataset_id"][:20]}…</div>',
                unsafe_allow_html=True,
            )


def require_session() -> bool:
    """
    Returns True if a session with an ingested dataset exists.
    Otherwise shows a warning and returns False.
    """
    if not st.session_state.get("session_id") or not st.session_state.get("dataset_id"):
        st.warning(
            "⚠️ No active dataset. Go to **01 Ingest** to upload a file or connect to a database.",
            icon="⚠️",
        )
        return False
    return True
