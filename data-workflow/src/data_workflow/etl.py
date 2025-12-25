import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from data_workflow.io import read_orders_csv, read_users_csv, write_parquet
from data_workflow.joins import safe_left_join
from data_workflow.quality import assert_non_empty, assert_unique_key, require_columns
from data_workflow.transforms import (add_missing_flags,add_outlier_flag,add_time_parts,apply_mapping,enforce_schema,normalize_text,parse_datetime,winsorize,)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders_df = read_orders_csv(cfg.raw_orders)
    users_df = read_users_csv(cfg.raw_users)
    return orders_df, users_df


def transform(orders_raw_df: pd.DataFrame, users_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    require_columns(orders_raw_df,["order_id", "user_id", "amount", "quantity", "created_at", "status"],)
    require_columns(users_df, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw_df, "orders_raw_df")
    assert_non_empty(users_df, "users_df")
    assert_unique_key(users_df, "user_id")

    ordersdf = enforce_schema(orders_raw_df)

    status_normalized = normalize_text(ordersdf["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}

    ordersdf = ordersdf.assign(status_clean=apply_mapping(status_normalized, mapping))
    ordersdf = add_missing_flags(ordersdf, cols=["amount", "quantity"])
    ordersdf = parse_datetime(ordersdf, col="created_at", utc=True)
    ordersdf = add_time_parts(ordersdf, ts_col="created_at")

    joined_df = safe_left_join(ordersdf,users_df,on="user_id",validate="many_to_one",suffixes=("", "_user"),)
    if len(joined_df) != len(ordersdf):
        raise AssertionError("Row count changed (join explosion?)")

    joined_df = joined_df.assign(amount_winsor=winsorize(joined_df["amount"]))
    joined_df = add_outlier_flag(joined_df, "amount", k=1.5)

    orders_clean_df = joined_df.copy()
    analytics_df = joined_df.copy()
    return orders_clean_df, analytics_df


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_outputs(orders_clean_df: pd.DataFrame,users_df: pd.DataFrame,analytics_df: pd.DataFrame,cfg: ETLConfig,) -> None:
    _ensure_parent_dir(cfg.out_orders_clean)
    _ensure_parent_dir(cfg.out_users)
    _ensure_parent_dir(cfg.out_analytics)

    write_parquet(orders_clean_df, cfg.out_orders_clean)
    write_parquet(users_df, cfg.out_users)
    write_parquet(analytics_df, cfg.out_analytics)


def write_run_meta(cfg: ETLConfig, *, analytics_df: pd.DataFrame) -> None:
    missing_created_at = int(analytics_df["created_at"].isna().sum()) if "created_at" in analytics_df.columns else 0
    country_match_rate = (
        1.0 - float(analytics_df["country"].isna().mean())
        if "country" in analytics_df.columns and len(analytics_df) > 0
        else 0.0
    )

    metadata = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows_out": int(len(analytics_df)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    _ensure_parent_dir(cfg.run_meta)
    cfg.run_meta.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    logger.info("Loading inputs")
    orders_raw_df, users_df = load_inputs(cfg)

    logger.info("Transforming (orders=%s, users=%s)", len(orders_raw_df), len(users_df))
    orders_clean_df, analytics_df = transform(orders_raw_df, users_df)

    logger.info("Writing outputs")
    load_outputs(orders_clean_df, users_df, analytics_df, cfg)

    logger.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, analytics_df=analytics_df)

    logger.info("ETL complete: %s rows in analytics table", len(analytics_df))