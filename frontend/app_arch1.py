"""
frontend/app.py

DQ Agent — Streamlit entry point.

Configures the global page layout, injects Deloitte brand CSS, renders
the sidebar navigation, and provides the Role / Username controls that
propagate to all API calls via session state.

Navigation is handled by Streamlit's built-in multi-page system:
    pages/01_ingest.py      — Upload file or connect to DB
    pages/02_review.py      — Review & approve/reject issues
    pages/03_export.py      — Export clean dataset
    pages/04_scorecard.py   — DQ scorecard dashboard
    pages/05_audit_log.py   — Immutable audit trail
"""

import streamlit as st

from components.ui import (
    DL_DARK_GRAY,
    DL_GREEN,
    DL_WHITE,
    inject_brand_css,
    init_session_state,
    sidebar_session_info,
)

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="DQ Agent | Deloitte",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_brand_css()
init_session_state()

# ---------------------------------------------------------------------------
# Sidebar — branding + user controls
# ---------------------------------------------------------------------------

with st.sidebar:
    # Deloitte wordmark + product name
    st.markdown(
        f"""
        <div style="padding:16px 0 8px 0;">
            <div style="font-size:1.4rem;font-weight:700;color:{DL_WHITE};
                        letter-spacing:-0.5px;line-height:1.1;">
                Deloitte
            </div>
            <div style="font-size:0.7rem;font-weight:600;color:{DL_GREEN};
                        text-transform:uppercase;letter-spacing:0.12em;margin-top:2px;">
                Data Quality Agent
            </div>
            <div style="height:2px;background:{DL_GREEN};margin-top:10px;border-radius:1px;"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("##### Navigation")
    st.page_link("app.py",                   label="🏠 Home",          )
    st.page_link("pages/01_ingest.py",       label="📥 01 · Ingest",   )
    st.page_link("pages/02_review.py",       label="🔎 02 · Review",   )
    st.page_link("pages/03_export.py",       label="📤 03 · Export",   )
    st.page_link("pages/04_scorecard.py",    label="📊 04 · Scorecard",)
    st.page_link("pages/05_audit_log.py",    label="📋 05 · Audit Log",)

    st.markdown("---")
    st.markdown(
        '<div style="font-size:0.7rem;color:#A0A0A0;text-transform:uppercase;'
        'letter-spacing:0.08em;margin-bottom:6px;">User settings</div>',
        unsafe_allow_html=True,
    )

    st.session_state["role"] = st.selectbox(
        "Role",
        options=["viewer", "analyst", "senior analyst", "admin"],
        index=["viewer", "analyst", "senior analyst", "admin"].index(
            st.session_state.get("role", "analyst")
        ),
        key="role_select",
        label_visibility="collapsed",
    )
    st.session_state["username"] = st.text_input(
        "Username",
        value=st.session_state.get("username", "analyst@deloitte.com"),
        key="username_input",
        label_visibility="collapsed",
        placeholder="username@deloitte.com",
    )

    sidebar_session_info()

    st.markdown("---")
    st.markdown(
        f'<div style="font-size:0.65rem;color:#555555;text-align:center;">'
        f'<em>Together makes progress</em></div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Home page content
# ---------------------------------------------------------------------------

st.markdown(
    f"""
    <div style="padding:32px 0 16px 0;">
        <div style="font-size:2rem;font-weight:300;color:{DL_DARK_GRAY};line-height:1.2;">
            AI-powered <span style="font-style:italic;font-weight:700;color:{DL_GREEN};">data quality</span>
            <br>for FP&A pipelines
        </div>
        <div style="margin-top:8px;font-size:0.95rem;color:#555555;max-width:520px;">
            Ingest your vendor master, invoice feeds, or budget uploads.
            Let the agent surface quality issues, propose fixes, and — with your
            approval — produce a certified clean dataset.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Workflow steps
cols = st.columns(5, gap="small")
steps = [
    ("📥", "01", "Ingest",    "Upload a file or connect to a live database."),
    ("🔎", "02", "Review",    "Inspect AI-flagged issues and approve or reject fixes."),
    ("📤", "03", "Export",    "Write the certified clean dataset to a file or table."),
    ("📊", "04", "Scorecard", "See your data quality score by category."),
    ("📋", "05", "Audit Log", "Immutable record of every decision made."),
]

for col, (icon, num, name, desc) in zip(cols, steps):
    with col:
        st.markdown(
            f"""
            <div style="background:{DL_WHITE};border:1px solid #E6E6E6;
                        border-top:3px solid {DL_GREEN};border-radius:4px;
                        padding:16px 14px;min-height:130px;">
                <div style="font-size:1.5rem;">{icon}</div>
                <div style="font-size:0.65rem;font-weight:700;color:{DL_GREEN};
                            text-transform:uppercase;letter-spacing:0.1em;margin:6px 0 2px;">
                    Step {num}
                </div>
                <div style="font-size:0.9rem;font-weight:700;color:{DL_DARK_GRAY};">
                    {name}
                </div>
                <div style="font-size:0.78rem;color:#555555;margin-top:4px;line-height:1.4;">
                    {desc}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# Session status strip
st.markdown("<br>", unsafe_allow_html=True)
if st.session_state.get("session_id"):
    n_issues = len(st.session_state.get("issues", []))
    st.success(
        f"✅ Active session · Dataset: `{st.session_state.get('dataset_id', '—')}` · "
        f"{n_issues} issue(s) loaded",
    )
    if st.button("🗑 Clear session and start over", key="home_clear"):
        for key in ["session_id", "dataset_id", "ingest_summary",
                    "conversation", "issues", "source_type",
                    "export_done", "export_path", "export_table"]:
            st.session_state[key] = None if key != "conversation" and key != "issues" else []
        st.rerun()
else:
    st.info("No active session. Navigate to **01 · Ingest** to begin.")
