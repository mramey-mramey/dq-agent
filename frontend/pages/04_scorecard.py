"""
frontend/pages/04_scorecard.py

Step 04 — DQ Scorecard

Visual dashboard showing:
  • Overall data quality score (0–100)
  • Issue counts by severity and status
  • Per-category breakdown table
  • Deloitte-branded bar charts using brand color sequence
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
    api_scorecard,
    inject_brand_css,
    init_session_state,
    render_metric_card,
    render_page_header,
    render_section_header,
    require_session,
    sidebar_session_info,
)

st.set_page_config(page_title="Scorecard · DQ Agent", page_icon="📊", layout="wide")
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
    "📊 DQ Scorecard",
    "Your data quality score is the percentage of rows free of HIGH or CRITICAL issues.",
)

if not require_session():
    st.stop()

sid = st.session_state["session_id"]

# ── Load scorecard ────────────────────────────────────────────────────────────
if st.button("🔄 Refresh Scorecard", key="refresh_score"):
    st.cache_data.clear()

@st.cache_data(ttl=10, show_spinner=False)
def _load_scorecard(_sid: str) -> dict | None:
    try:
        return api_scorecard(_sid)
    except APIError as e:
        st.error(f"Could not load scorecard: {e.detail}")
        return None

sc = _load_scorecard(sid)
if sc is None:
    st.stop()

score = sc.get("overall_score", 100.0)
total_issues = sc.get("total_issues", 0)
rows_with_issues = sc.get("rows_with_issues", 0)
total_rows = sc.get("total_rows", 0)
sev = sc.get("severity_breakdown", {})
stat = sc.get("status_breakdown", {})
by_cat = sc.get("by_category", {})

# ── Score ring ────────────────────────────────────────────────────────────────
render_section_header("Overall Score")

score_col, detail_col = st.columns([1, 3], gap="large")

with score_col:
    score_color = DL_GREEN if score >= 90 else DL_AMBER if score >= 70 else DL_RED
    arc_pct = int(score)
    # SVG score gauge
    st.markdown(
        f"""
        <div style="text-align:center;padding:20px;">
            <svg viewBox="0 0 200 130" width="220" xmlns="http://www.w3.org/2000/svg">
                <!-- Background arc track -->
                <path d="M 20 110 A 80 80 0 0 1 180 110"
                      fill="none" stroke="{DL_GRAY}" stroke-width="16"
                      stroke-linecap="round"/>
                <!-- Score arc -->
                <path d="M 20 110 A 80 80 0 0 1 180 110"
                      fill="none" stroke="{score_color}" stroke-width="16"
                      stroke-linecap="round"
                      stroke-dasharray="{arc_pct * 2.51} 251"
                      transform="rotate(0,100,110)"/>
                <!-- Score text -->
                <text x="100" y="105" text-anchor="middle"
                      font-family="Open Sans, sans-serif"
                      font-size="38" font-weight="700" fill="{score_color}">{score:.0f}</text>
                <text x="100" y="126" text-anchor="middle"
                      font-family="Open Sans, sans-serif"
                      font-size="13" fill="#555555">out of 100</text>
            </svg>
        </div>
        """,
        unsafe_allow_html=True,
    )
    label = "Excellent" if score >= 95 else "Good" if score >= 85 else "Needs Attention" if score >= 70 else "Critical"
    st.markdown(
        f'<div style="text-align:center;font-size:0.85rem;font-weight:700;'
        f'color:{score_color};margin-top:-8px;">{label}</div>',
        unsafe_allow_html=True,
    )

with detail_col:
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        render_metric_card("Total Issues", str(total_issues))
    with m2:
        render_metric_card(
            "Rows Affected",
            str(rows_with_issues),
            sub=f"of {total_rows:,} total",
            color="red" if rows_with_issues > 0 else "green",
        )
    with m3:
        render_metric_card(
            "Critical + High",
            str(sev.get("CRITICAL", 0) + sev.get("HIGH", 0)),
            color="red" if (sev.get("CRITICAL", 0) + sev.get("HIGH", 0)) > 0 else "green",
        )
    with m4:
        executed = stat.get("EXECUTED", 0)
        render_metric_card(
            "Fixes Executed",
            str(executed),
            color="green" if executed > 0 else "",
        )

# ── Severity breakdown ────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
render_section_header("Severity Breakdown")

sev_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
sev_colors_list = [DL_RED, DL_AMBER, DL_BLUE, DL_GREEN]

if total_issues > 0:
    sev_col1, sev_col2 = st.columns([2, 1], gap="large")

    with sev_col1:
        # Horizontal bar chart built with HTML
        for sev_level, color in zip(sev_order, sev_colors_list):
            count = sev.get(sev_level, 0)
            pct = (count / total_issues * 100) if total_issues > 0 else 0
            st.markdown(
                f"""
                <div style="margin-bottom:12px;">
                    <div style="display:flex;justify-content:space-between;
                                font-size:0.78rem;font-weight:600;margin-bottom:4px;">
                        <span style="color:{DL_DARK_GRAY};">{sev_level}</span>
                        <span style="color:{color};">{count}</span>
                    </div>
                    <div style="background:{DL_GRAY};border-radius:3px;height:10px;">
                        <div style="background:{color};width:{pct:.1f}%;
                                    height:10px;border-radius:3px;
                                    transition:width 0.3s;"></div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with sev_col2:
        render_section_header("Status Breakdown")
        stat_order = ["OPEN", "PENDING_APPROVAL", "APPROVED", "REJECTED", "EXECUTED"]
        stat_colors_map = {
            "OPEN": "#888",
            "PENDING_APPROVAL": DL_AMBER,
            "APPROVED": DL_GREEN,
            "REJECTED": DL_RED,
            "EXECUTED": DL_BLUE,
        }
        for s in stat_order:
            cnt = stat.get(s, 0)
            label = s.replace("_", " ")
            st.markdown(
                f'<div style="display:flex;justify-content:space-between;'
                f'padding:6px 0;border-bottom:1px solid {DL_GRAY};">'
                f'<span style="font-size:0.8rem;color:#555;">{label}</span>'
                f'<span style="font-size:0.8rem;font-weight:700;'
                f'color:{stat_colors_map[s]};">{cnt}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
else:
    st.success("🟢 No issues found — your dataset passed all quality checks.")

# ── Per-category breakdown ────────────────────────────────────────────────────
if by_cat:
    st.markdown("<br>", unsafe_allow_html=True)
    render_section_header("Issues by Category")

    # Build table rows
    rows_html = ""
    for cat, detail in sorted(by_cat.items(), key=lambda x: -x[1]["total_issues"]):
        total   = detail["total_issues"]
        hc      = detail["high_critical_issues"]
        aff_rows = detail["affected_row_count"]
        hc_pct  = (hc / total * 100) if total > 0 else 0
        row_color = DL_RED if hc > 0 else DL_DARK_GRAY
        rows_html += f"""
        <tr style="border-bottom:1px solid {DL_GRAY};">
            <td style="padding:8px 12px;font-weight:600;color:{DL_DARK_GRAY};
                       font-size:0.82rem;">{cat}</td>
            <td style="padding:8px 12px;text-align:center;font-size:0.82rem;">{total}</td>
            <td style="padding:8px 12px;text-align:center;font-size:0.82rem;
                       color:{row_color};font-weight:{'700' if hc > 0 else '400'};">{hc}</td>
            <td style="padding:8px 12px;text-align:center;font-size:0.82rem;">{aff_rows:,}</td>
            <td style="padding:8px 12px;">
                <div style="background:{DL_GRAY};border-radius:3px;height:6px;">
                    <div style="background:{DL_RED if hc > 0 else DL_GREEN};
                                width:{hc_pct:.0f}%;height:6px;border-radius:3px;"></div>
                </div>
            </td>
        </tr>"""

    st.markdown(
        f"""
        <table style="width:100%;border-collapse:collapse;font-family:'Open Sans',sans-serif;">
            <thead>
                <tr style="background:{DL_DARK_GRAY};color:white;">
                    <th style="padding:10px 12px;text-align:left;font-size:0.78rem;
                               text-transform:uppercase;letter-spacing:0.06em;">Category</th>
                    <th style="padding:10px 12px;text-align:center;font-size:0.78rem;
                               text-transform:uppercase;letter-spacing:0.06em;">Total</th>
                    <th style="padding:10px 12px;text-align:center;font-size:0.78rem;
                               text-transform:uppercase;letter-spacing:0.06em;">High+Critical</th>
                    <th style="padding:10px 12px;text-align:center;font-size:0.78rem;
                               text-transform:uppercase;letter-spacing:0.06em;">Rows Affected</th>
                    <th style="padding:10px 12px;font-size:0.78rem;
                               text-transform:uppercase;letter-spacing:0.06em;">H+C Ratio</th>
                </tr>
            </thead>
            <tbody style="background:{DL_WHITE};">
                {rows_html}
            </tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )

# ── Narrative summary ─────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
render_section_header("Interpretation")
if score >= 95:
    interp = (
        f"Your dataset scores **{score:.0f}/100** — excellent quality. "
        f"Less than {100 - score:.0f}% of rows have significant issues. "
        "This dataset is ready for downstream use with minimal risk."
    )
elif score >= 80:
    interp = (
        f"Your dataset scores **{score:.0f}/100** — good quality with some issues to address. "
        f"{rows_with_issues:,} row(s) have HIGH or CRITICAL findings. "
        "Review the issue list and execute approved fixes before loading downstream."
    )
elif score >= 60:
    interp = (
        f"Your dataset scores **{score:.0f}/100** — quality needs attention. "
        f"{rows_with_issues:,} row(s) ({rows_with_issues / max(total_rows,1)*100:.0f}%) "
        "have significant issues. Resolve HIGH and CRITICAL items before loading downstream."
    )
else:
    interp = (
        f"Your dataset scores **{score:.0f}/100** — critical quality problems detected. "
        f"{rows_with_issues:,} of {total_rows:,} rows are affected by HIGH or CRITICAL issues. "
        "Do not load this dataset downstream until major issues are resolved."
    )
st.markdown(interp)
