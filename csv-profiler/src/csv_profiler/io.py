from __future__ import annotations
from csv import DictReader
from pathlib import Path


def read_csv_rows(path: str | Path) -> list[dict[str, str]]:
    """Read a CSV file into a list of rows (dicts of strings)."""
    path = Path(path)
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = DictReader(f)
        return [dict(row) for row in reader]
