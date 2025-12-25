# scripts/run_day2_clean.py
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from data_workflow.config import make_paths
from data_workflow.io import read_orders_csv, read_users_csv, write_parquet
from data_workflow.quality import assert_in_range, assert_non_empty, require_columns
from data_workflow.transforms import (
    add_missing_flags,
    enforce_schema,
    missingness_report,
    normalize_text,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def main() -> None:
    paths = make_paths(ROOT)

    orders_path = paths.raw / "orders.csv"
    users_path = paths.raw / "users.csv"

    logging.info(f"Reading raw orders: {orders_path}")
    orders = read_orders_csv(orders_path)

    logging.info(f"Reading raw users: {users_path}")
    users = read_users_csv(users_path)

    # Quality checks (fail fast)
    require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders, name="orders")
    assert_non_empty(users, name="users")

    # Enforce schema (types)
    orders_t = enforce_schema(orders)

    # Missingness report -> save to reports/
    rep = missingness_report(orders_t)
    out_missing = ROOT / "reports" / "missingness_orders.csv"
    out_missing.parent.mkdir(parents=True, exist_ok=True)
    rep.to_csv(out_missing, index=True)
    logging.info(f"Missingness report saved: {out_missing}")

    # Normalize status -> create status_clean
    orders_t = orders_t.assign(status_clean=normalize_text(orders_t["status"]))

    # Add missing flags
    orders_t = add_missing_flags(orders_t, ["amount", "quantity"])

    # Range checks (ignore missing automatically)
    assert_in_range(orders_t["amount"], lo=0, name="amount")
    assert_in_range(orders_t["quantity"], lo=0, name="quantity")

    # Write clean parquet
    out_clean = paths.processed / "orders_clean.parquet"
    write_parquet(orders_t, out_clean)
    logging.info(f"Clean parquet written: {out_clean}")
    logging.info(f"Done. Rows: {len(orders_t):,} | Cols: {orders_t.shape[1]}")


if __name__ == "__main__":
    main()
