"""
frontend/pages/02_review.py

Step 02 — Review Issues

Shows all DQ issues surfaced by the agent in card format.
Analysts can:
  • Approve or reject individual issues
  • Bulk-approve high-confidence non-dedup issues (Senior Analyst+)
  • Execute all approved issues in one click
  • Filter by severity, category, or status
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
    SEVERITY_ICONS,
    STATUS_COLORS,
    STATUS_ICONS,
    APIError,
    api_approve_issue,
    api_bulk_approve,
    api_execute,
    api_list_issues,
    api_reject_issue,
    inject_brand_css,
    init_session_state,
    render_confidence_bar,
    render_page_header,
    render_section_header,
    render_severity_badge,
    render_status_badge,
    require_session,
    sidebar_session_info,
)

st.set_page_config(page_title="Review · DQ Agent", page_icon="🔎", layout="wide")
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

    st.markdown("---")
    render_section_header("Filters")
    sev_filter = st.multiselect(
        "Severity",
        options=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        default=[],
        key="sev_filter",
    )
    cat_filter = st.multiselect(
        "Category",
        options=["COMPLETENESS", "UNIQUENESS", "DEDUPLICATION",
                 "FORMAT_VALIDITY", "REFERENTIAL_INTEGRITY",
                 "CONSISTENCY", "OUTLIER", "DOMAIN_RULE"],
        default=[],
        key="cat_filter",
    )
    status_filter = st.selectbox(
        "Status",
        options=["All", "OPEN", "PENDING_APPROVAL", "APPROVED", "REJECTED", "EXECUTED"],
        key="status_filter",
    )
    sidebar_session_info()

render_page_header(
    "🔎 Review Issues",
    "Inspect AI-flagged data quality issues. Approve fixes you agree with, reject those you don't.",
)

if not require_session():
    st.stop()

sid = st.session_state["session_id"]
username = st.session_state.get("username", "analyst")
role = st.session_state.get("role", "analyst")

# ── Load issues ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=5, show_spinner=False)
def _load_issues(_sid: str) -> list[dict]:
    try:
        return api_list_issues(_sid)
    except APIError as e:
        st.error(f"Could not load issues: {e.detail}")
        return []


if st.button("🔄 Refresh", key="refresh_issues"):
    st.cache_data.clear()

all_issues = _load_issues(sid)

# Apply sidebar filters
def _apply_filters(issues: list[dict]) -> list[dict]:
    result = issues
    if sev_filter:
        result = [i for i in result if i["severity"] in sev_filter]
    if cat_filter:
        result = [i for i in result if i["category"] in cat_filter]
    if status_filter != "All":
        result = [i for i in result if i["status"] == status_filter]
    return result

issues = _apply_filters(all_issues)

# ── Summary strip ─────────────────────────────────────────────────────────────
counts = {s: sum(1 for i in all_issues if i["status"] == s)
          for s in ["OPEN", "APPROVED", "REJECTED", "EXECUTED"]}
sev_counts = {s: sum(1 for i in all_issues if i["severity"] == s)
              for s in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]}

cols = st.columns(7, gap="small")
stat_items = [
    ("Total", str(len(all_issues)),      DL_DARK_GRAY),
    ("Open",  str(counts["OPEN"]),       "#888888"),
    ("Approved", str(counts["APPROVED"]), DL_GREEN),
    ("Rejected", str(counts["REJECTED"]), DL_RED),
    ("Executed", str(counts["EXECUTED"]), DL_BLUE),
    ("Critical+High", str(sev_counts.get("CRITICAL", 0) + sev_counts.get("HIGH", 0)), DL_AMBER),
    ("Low+Medium", str(sev_counts.get("LOW", 0) + sev_counts.get("MEDIUM", 0)), DL_DARK_GRAY),
]
for col, (label, value, color) in zip(cols, stat_items):
    with col:
        st.markdown(
            f'<div style="background:{DL_WHITE};border:1px solid {DL_GRAY};'
            f'border-top:3px solid {color};border-radius:4px;padding:10px 12px;'
            f'text-align:center;">'
            f'<div style="font-size:0.65rem;font-weight:600;color:#555;'
            f'text-transform:uppercase;letter-spacing:0.06em;">{label}</div>'
            f'<div style="font-size:1.6rem;font-weight:700;color:{color};">{value}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

st.markdown("<br>", unsafe_allow_html=True)

# ── Bulk actions bar ──────────────────────────────────────────────────────────
render_section_header("Bulk Actions")
bulk_col1, bulk_col2, bulk_col3 = st.columns([2, 2, 3])

with bulk_col1:
    bulk_threshold = st.slider(
        "Bulk-approve confidence threshold",
        min_value=0.80,
        max_value=1.00,
        value=0.95,
        step=0.01,
        format="%.2f",
        key="bulk_threshold",
        help="Issues above this confidence AND not DEDUPLICATION/UNIQUENESS will be approved.",
        disabled=role not in ("senior analyst", "admin"),
    )

with bulk_col2:
    eligible = [i for i in all_issues
                if i.get("can_bulk_approve", False)
                and i["status"] in ("OPEN", "PENDING_APPROVAL")]
    st.metric("Eligible for bulk approve", len(eligible))

with bulk_col3:
    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button(
            f"✅ Bulk Approve ({len(eligible)})",
            key="bulk_approve_btn",
            disabled=role not in ("senior analyst", "admin") or len(eligible) == 0,
            help="Requires Senior Analyst role.",
        ):
            try:
                result = api_bulk_approve(sid, username, bulk_threshold)
                st.success(f"Approved {result['approved_count']} issue(s).")
                st.cache_data.clear()
                st.rerun()
            except APIError as e:
                st.error(f"Bulk approve failed: {e.detail}")

    with bc2:
        approved_count = sum(1 for i in all_issues if i["status"] == "APPROVED")
        if st.button(
            f"⚡ Execute All Approved ({approved_count})",
            key="execute_all_btn",
            disabled=approved_count == 0,
        ):
            with st.spinner("Executing approved fixes…"):
                try:
                    result = api_execute(sid, username)
                    st.success(
                        f"Executed {result['succeeded']} fix(es). "
                        f"{result['failed']} failed. "
                        f"{result['skipped']} skipped."
                    )
                    st.cache_data.clear()
                    st.rerun()
                except APIError as e:
                    st.error(f"Execute failed: {e.detail}")

st.markdown("<br>", unsafe_allow_html=True)

# ── Issue cards ───────────────────────────────────────────────────────────────
if not issues:
    if not all_issues:
        st.info("No issues found. Run quality checks from the **01 · Ingest** page first.")
    else:
        st.info("No issues match the current filters.")
    st.stop()

render_section_header(f"Issues ({len(issues)} shown of {len(all_issues)} total)")

for issue in issues:
    sev   = issue["severity"]
    stat  = issue["status"]
    cat   = issue["category"]
    iid   = issue["issue_id"]
    conf  = issue.get("confidence", 1.0)
    cols_a  = issue.get("affected_columns", [])
    is_act  = issue.get("is_actionable", False)

    # Card container
    border_color = {
        "CRITICAL": DL_RED,
        "HIGH": DL_AMBER,
        "MEDIUM": DL_BLUE,
        "LOW": DL_GREEN,
    }.get(sev, DL_GRAY)

    st.markdown(
        f'<div class="dl-issue-card {sev}" style="border-left-color:{border_color};">',
        unsafe_allow_html=True,
    )

    top_left, top_right = st.columns([5, 2])
    with top_left:
        badges = render_severity_badge(sev) + " " + render_status_badge(stat)
        cat_pill = (
            f'<span style="background:#F0F0F0;border:1px solid #DDD;'
            f'border-radius:3px;font-size:0.68rem;font-weight:600;'
            f'padding:2px 7px;color:#555;text-transform:uppercase;'
            f'letter-spacing:0.04em;">{cat}</span>'
        )
        st.markdown(
            f'<div style="margin-bottom:6px;">{badges} {cat_pill}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="font-size:0.88rem;color:{DL_DARK_GRAY};line-height:1.5;">'
            f'{issue["description"]}</div>',
            unsafe_allow_html=True,
        )
        if cols_a:
            st.markdown(
                f'<div style="font-size:0.75rem;color:#555;margin-top:6px;">'
                f'<b>Columns:</b> {", ".join(cols_a)}</div>',
                unsafe_allow_html=True,
            )

    with top_right:
        st.markdown(
            f'<div style="font-size:0.7rem;color:#555;margin-bottom:4px;'
            f'text-align:right;">Confidence</div>',
            unsafe_allow_html=True,
        )
        st.markdown(render_confidence_bar(conf), unsafe_allow_html=True)
        st.markdown(
            f'<div style="font-size:0.7rem;color:#555;margin-top:8px;'
            f'text-align:right;">Issue ID: <code>{iid}</code></div>',
            unsafe_allow_html=True,
        )
        if not is_act:
            st.markdown(
                f'<div style="font-size:0.7rem;color:{DL_AMBER};text-align:right;'
                f'margin-top:4px;">⚑ Manual fix required</div>',
                unsafe_allow_html=True,
            )

    st.markdown('</div>', unsafe_allow_html=True)

    # Action row (only for actionable, non-terminal issues)
    if stat in ("OPEN", "PENDING_APPROVAL") and is_act:
        can_approve = (
            (role in ("analyst", "senior analyst", "admin") and sev in ("LOW", "MEDIUM"))
            or (role in ("senior analyst", "admin") and sev in ("HIGH", "CRITICAL"))
        )
        act1, act2, act3, _ = st.columns([1.2, 1.2, 1.5, 4])

        with act1:
            if st.button(
                "✅ Approve",
                key=f"approve_{iid}",
                disabled=not can_approve,
                help="Approves this fix. Execution is a separate step." if can_approve
                     else f"Requires {'Senior Analyst' if sev in ('HIGH','CRITICAL') else 'Analyst'} role.",
            ):
                try:
                    api_approve_issue(sid, iid, username)
                    st.cache_data.clear()
                    st.rerun()
                except APIError as e:
                    st.error(f"Approve failed: {e.detail}")

        with act2:
            if st.button("✗ Reject", key=f"reject_{iid}"):
                try:
                    api_reject_issue(sid, iid, username, note="Rejected via UI.")
                    st.cache_data.clear()
                    st.rerun()
                except APIError as e:
                    st.error(f"Reject failed: {e.detail}")

        with act3:
            with st.expander("Add note"):
                note_text = st.text_input(
                    "Note",
                    key=f"note_{iid}",
                    label_visibility="collapsed",
                    placeholder="Optional reviewer note…",
                )
                if st.button("Save note", key=f"note_save_{iid}"):
                    try:
                        api_approve_issue(sid, iid, username, note=note_text)
                        st.cache_data.clear()
                        st.rerun()
                    except APIError as e:
                        st.error(e.detail)

    elif stat == "APPROVED":
        st.markdown(
            f'<div style="font-size:0.78rem;color:{DL_GREEN};font-weight:600;'
            f'padding:4px 0 8px 0;">✅ Approved — will execute on next "Execute All Approved"</div>',
            unsafe_allow_html=True,
        )
    elif stat == "EXECUTED":
        st.markdown(
            f'<div style="font-size:0.78rem;color:{DL_BLUE};font-weight:600;'
            f'padding:4px 0 8px 0;">⚡ Executed — fix has been applied to the dataset</div>',
            unsafe_allow_html=True,
        )
    elif stat == "REJECTED":
        st.markdown(
            f'<div style="font-size:0.78rem;color:{DL_RED};font-weight:600;'
            f'padding:4px 0 8px 0;">✗ Rejected — no change made</div>',
            unsafe_allow_html=True,
        )

    st.markdown("<hr style='border:none;border-top:1px solid #F0F0F0;margin:4px 0 12px;'>",
                unsafe_allow_html=True)
