import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_workflow.config import make_paths
from data_workflow.io import read_orders_csv, read_users_csv, write_parquet
from data_workflow.transforms import enforce_schema


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s",
)


def main() -> None:
    paths = make_paths(ROOT)

    orders_path = paths.raw / "orders.csv"
    users_path = paths.raw / "users.csv"

    logging.info(f"Loading orders from: {orders_path}")
    orders = read_orders_csv(orders_path)

    logging.info(f"Loading users from: {users_path}")
    users = read_users_csv(users_path)

    # Apply schema enforcement on orders
    orders_clean = enforce_schema(orders)

    # Output parquet paths
    out_orders = paths.processed / "orders.parquet"
    out_users = paths.processed / "users.parquet"

    write_parquet(orders_clean, out_orders)
    write_parquet(users, out_users)

    logging.info(f"Orders rows: {len(orders_clean):,} | Saved: {out_orders}")
    logging.info(f"Users rows: {len(users):,} | Saved: {out_users}")


if __name__ == "__main__":
    main()
