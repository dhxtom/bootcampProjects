from __future__ import annotations

from collections import Counter


MISSING = {"", "na", "n/a", "null", "none", "nan"}


def is_missing(value: str | None) -> bool:
    if value is None:
        return True
    cleaned = value.strip().casefold()
    return cleaned in MISSING


def try_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def infer_type(values: list[str]) -> str:
    usable = [v for v in values if not is_missing(v)]
    if not usable:
        return "text"
    for v in usable:
        if try_float(v.strip()) is None:
            return "text"
    return "number"


def basic_profile(rows: list[dict[str, str]]) -> dict:
    # لو الملف فاضي
    if not rows:
        return {
            "rows": 0,
            "columns": [],
            "notes": ["Empty dataset"],
        }

    columns = list(rows[0].keys())
    n_rows = len(rows)

    col_reports: list[dict] = []

    for col in columns:
        raw_vals = [(row.get(col) or "") for row in rows]
        missing = sum(1 for v in raw_vals if is_missing(v))
        non_missing_vals = [v.strip() for v in raw_vals if not is_missing(v)]
        unique = len(set(non_missing_vals))

        col_type = infer_type(raw_vals)

        col_info: dict = {
            "name": col,
            "type": col_type,
            "total": n_rows,
            "missing": missing,
            "missing_pct": 0.0 if n_rows == 0 else (100.0 * missing / n_rows),
            "unique": unique,
        }

        # لو العمود رقمي: احسب min/max/mean
        if col_type == "number" and non_missing_vals:
            nums = [try_float(v) for v in non_missing_vals]
            nums = [x for x in nums if x is not None]
            if nums:
                col_info["min"] = min(nums)
                col_info["max"] = max(nums)
                col_info["mean"] = sum(nums) / len(nums)

        # لو نصي: اعرض أكثر 5 قيم تكراراً
        if col_type == "text" and non_missing_vals:
            top = Counter(non_missing_vals).most_common(5)
            col_info["top_values"] = [{"value": v, "count": c} for v, c in top]

        col_reports.append(col_info)

    return {
        "rows": n_rows,
        "n_columns": len(columns),
        "columns": col_reports,   # قائمة قواميس لكل عمود
    }


def profile_rows(rows: list[dict[str, str]]) -> dict:
    # هذه هي الدالة اللي Streamlit والـ CLI ينادونها
    return basic_profile(rows)
