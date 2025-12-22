from __future__ import annotations
import json
from pathlib import Path


def write_json(report: dict, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def write_markdown(report: dict, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    summary = report["summary"]
    columns = report["columns"]

    lines: list[str] = []

    # Header
    lines.append("# CSV Profiling Report")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Rows: **{summary['rows']}**")
    lines.append(f"- Columns: **{summary['columns']}**")
    lines.append("")

    # Table
    lines.append("## Columns Overview")
    lines.append("")
    lines.append("| Column | Type | Missing | Missing % | Unique |")
    lines.append("|---|---|---:|---:|---:|")

    for name, info in columns.items():
        lines.append(
            f"| {name} | {info['type']} | {info['missing']} | "
            f"{info['missing_pct']:.1%} | {info['unique']} |"
        )

    lines.append("")

    # Details
    lines.append("## Column Details")
    lines.append("")

    for name, info in columns.items():
        lines.append(f"### {name}")
        lines.append(f"- Type: **{info['type']}**")
        lines.append(f"- Missing: {info['missing']} ({info['missing_pct']:.1%})")
        lines.append(f"- Unique values: {info['unique']}")

        if info["type"] == "number":
            lines.append(f"- Min: {info.get('min')}")
            lines.append(f"- Max: {info.get('max')}")
            lines.append(f"- Mean: {info.get('mean'):.2f}")

        lines.append("")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
