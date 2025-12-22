from __future__ import annotations

import json
from pathlib import Path

import typer

from csv_profiler.io import read_csv_rows
from csv_profiler.profile import profile
from csv_profiler.render import write_json, write_markdown

app = typer.Typer(help="CSV Profiler CLI")


@app.command()
def profile_csv(
    csv_path: str = typer.Argument(..., help="Path to the CSV file"),
    out_dir: str = typer.Option("outputs", help="Output directory"),
):
    """
    Profile a CSV file and generate report.json and report.md
    """
    csv_path = Path(csv_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = read_csv_rows(csv_path)
    report = profile(rows)

    write_json(report, out_dir / "report.json")
    write_markdown(report, out_dir / "report.md")

    typer.echo(f"Report generated in: {out_dir}")


def main():
    app()


if __name__ == "__main__":
    main()
