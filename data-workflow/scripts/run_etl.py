from pathlib import Path
import sys


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


ROOT = get_project_root()
SOURCE = ROOT / "src"

if SOURCE.exists() and str(SOURCE) not in sys.path:
    sys.path.insert(0, str(SOURCE))

from data_workflow.etl import ETLConfig, run_etl  
def build_config(root: Path) -> ETLConfig:
    data_folder = root / "data"
    raw_folder = data_folder / "raw"
    processed_folder = data_folder / "processed"

    return ETLConfig(
        root=root,
        raw_orders=raw_folder / "orders.csv",
        raw_users=raw_folder / "users.csv",
        out_orders_clean=processed_folder / "orders_clean.parquet",
        out_users=processed_folder / "users.parquet",
        out_analytics=processed_folder / "analytics_table.parquet",
        run_meta=processed_folder / "_run_meta.json",
    )


if __name__ == "__main__":
    config = build_config(ROOT)
    run_etl(config)