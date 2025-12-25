from __future__ import annotations

from typing import Iterable
import pandas as pd


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    validate: str,
    suffixes: tuple[str, str] = ("_x", "_y"),) -> pd.DataFrame:
    merged = left.merge(right,how="left",on=on,validate=validate,suffixes=suffixes,
    )
    return merged