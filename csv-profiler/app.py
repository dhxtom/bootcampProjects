import csv
import json
from io import StringIO

import streamlit as st

from csv_profiler.profile import profile_rows

try:
    from csv_profiler.render import render_markdown
except Exception:
    render_markdown = None


def _fallback_markdown(report: dict) -> str:
    lines = []
    lines.append("# CSV Profiling Report")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Rows: **{report.get('rows', 0)}**")
    lines.append(f"- Columns: **{report.get('n_columns', 0)}**")
    lines.append("")
    lines.append("## Columns")
    lines.append("")
    lines.append("| Column | Type | Missing | Missing % | Unique |")
    lines.append("|---|---:|---:|---:|---:|")
    for c in report.get("columns", []):
        lines.append(
            f"| {c.get('name','')} | {c.get('type','')} | {c.get('missing',0)} | {c.get('missing_pct',0):.1f}% | {c.get('unique',0)} |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


st.set_page_config(page_title="CSV Profiler", layout="wide")
st.title("CSV Profiler")
st.caption("Upload a CSV → profile it → export JSON + Markdown")

st.sidebar.header("Inputs")
uploaded = st.sidebar.file_uploader("Upload CSV", type=["csv"])
show_preview = st.sidebar.checkbox("Show preview", value=True)

if uploaded is None:
    st.info("Upload a CSV file to begin.")
    st.stop()

text = uploaded.getvalue().decode("utf-8-sig")
rows = list(csv.DictReader(StringIO(text)))

st.write("Filename:", uploaded.name)
st.write("Rows loaded:", len(rows))

if show_preview:
    st.write(rows[:5])

if st.button("Generate report"):
    st.session_state["report"] = profile_rows(rows)

report = st.session_state.get("report")
if report is None:
    st.warning("Click Generate report to run profiling.")
    st.stop()

col1, col2 = st.columns(2)
col1.metric("Rows", report.get("rows", 0))
col2.metric("Columns", report.get("n_columns", 0))

st.subheader("Column profiles")
st.write(report.get("columns", []))

with st.expander("Raw JSON", expanded=False):
    st.json(report)

json_bytes = (json.dumps(report, indent=2, ensure_ascii=False) + "\n").encode("utf-8")

if render_markdown is not None:
    md_text = render_markdown(report)
else:
    md_text = _fallback_markdown(report)

st.download_button(
    "Download report.json",
    data=json_bytes,
    file_name="report.json",
    mime="application/json",
)

st.download_button(
    "Download report.md",
    data=md_text.encode("utf-8"),
    file_name="report.md",
    mime="text/markdown",
)
