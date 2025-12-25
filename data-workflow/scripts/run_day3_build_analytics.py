from pathlib import Path
import pandas as pd

from data_workflow.io import load_parquet, save_parquet
from data_workflow.quality import (validate_required_columns,validate_not_empty,validate_unique_column,assert_in_range,)
from data_workflow.transforms import (parse_datetime,add_time_parts,winsorize,add_outlier_flag,)
from data_workflow.joins import safe_left_join


def main() -> None:
    orders_path = Path("data/processed/orders_clean.parquet")
    users_path = Path("data/processed/users.parquet")
    result_path = Path("data/processed/analytics_table.parquet")

    ordersDF = load_parquet(orders_path)
    userDF = load_parquet(users_path)

    validate_not_empty(ordersDF, "orders")
    validate_not_empty(userDF, "users")

    orders_columns = ["order_id", "user_id", "amount", "quantity", "created_at"]
    validate_required_columns(ordersDF, orders_columns)
    validate_required_columns(userDF, ["user_id"])

    validate_unique_column(userDF, "user_id")

    ordersDF = parse_datetime(ordersDF, "created_at", utc=True)
    ordersDF = add_time_parts(ordersDF, "created_at")

    analytics_df = safe_left_join(
        left=ordersDF,
        right=userDF,
        on="user_id",
        validate="many_to_one",
        suffixes=("_order", "_user"),
    )

    analytics_df["amount_w"] = winsorize(analytics_df["amount"], lo=0.01, hi=0.99)
    analytics_df = add_outlier_flag(analytics_df, "amount", k=1.5)

    assert_in_range(analytics_df["amount_w"], min_value=0, value_name="amount_w")

    save_parquet(analytics_df, result_path)


if __name__ == "__main__":
    main()