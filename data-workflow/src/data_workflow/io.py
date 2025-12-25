from pathlib import Path
import pandas as pd

null_VAL = ["", "NA", "N/A", "null", "None"]


def load_orders_csv(csv_path: Path) -> pd.DataFrame:
   #Read Orders CSV file 
    dataframe = pd.read_csv(
        csv_path,
        dtype={ "order_id": "string","user_id": "string"},
        na_values=null_VAL,  
        keep_default_na=True,
    )
    return dataframe


def load_users_csv(csv_path: Path) -> pd.DataFrame:
   #Read Users CSV file
    dataframe = pd.read_csv(
        csv_path,
        dtype={"user_id": "string"},
        na_values=null_VAL,  
        keep_default_na=True,
    )
    return dataframe


def save_parquet(dataframe: pd.DataFrame, parquet_path: Path) -> None:
   # to save as Parquet
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(parquet_path, index=False)


def load_parquet(parquet_path: Path) -> pd.DataFrame:
  # load from Parquet
    dataframe = pd.read_parquet(parquet_path)
    return dataframe