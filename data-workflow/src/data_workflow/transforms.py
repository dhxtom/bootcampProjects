import re
import pandas as pd

_SPACE_PATTERN = re.compile(r"\s+")

# day 1 Implement
def enforce_schema(dataframe: pd.DataFrame) -> pd.DataFrame:
    # copy for  data 
    dataframe = dataframe.copy()
    
    dataframe["order_id"] = dataframe["order_id"].astype("string")
    dataframe["user_id"] = dataframe["user_id"].astype("string")
    dataframe["amount"] = pd.to_numeric(dataframe["amount"], errors="coerce").astype("Float64")
    dataframe["quantity"] = pd.to_numeric(dataframe["quantity"], errors="coerce").astype("Int64")
    return dataframe


def generate_missingness_report(dataframe: pd.DataFrame) -> pd.DataFrame:
    # report for missing data 
    missing_counts = dataframe.isna().sum()
    
    report = pd.DataFrame({
        "missing_count": missing_counts,
        "missing_percentage": missing_counts / len(dataframe)
    })
    return report.sort_values("missing_percentage", ascending=False)


def normalize_text_column(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip().str.lower()
    normalized = normalized.str.replace(_SPACE_PATTERN, " ", regex=True)
    return normalized

#  Day 3
def parse_datetime(dataframe:pd.DataFrame,col:str,*,utc: bool = True,) -> pd.DataFrame:
    datetime_series = pd.to_datetime(
        dataframe[col],
        errors="coerce",
        utc=utc,
    )
    return dataframe.assign(**{col: datetime_series})

def add_time_parts(dataframe: pd.DataFrame,ts_col: str,) -> pd.DataFrame:
    timestamp_series = dataframe[ts_col]
    return dataframe.assign(
        date=timestamp_series.dt.date,
        year=timestamp_series.dt.year,
        month=timestamp_series.dt.month,
        dow=timestamp_series.dt.dayofweek,
        hour=timestamp_series.dt.hour,
    )






# Add Outlier Helpers 
def iqr_bounds(series:pd.Series,k:float=1.5) -> tuple[float,float]:
    numeric_series = pd.to_numeric(series,errors="coerce")
    clean_values = numeric_series.dropna()

    q1 = clean_values.quantile(0.25)
    q3 = clean_values.quantile(0.75)
    iqr = q3 - q1

    lower = q1 - k * iqr
    upper = q3 + k * iqr
    
    return lower, upper



def winsorize(series:pd.Series, lo: float = 0.01, hi:float = 0.99) -> pd.Series:
    numeric_series = pd.to_numeric(series, errors="coerce")

    lower_limit = numeric_series.quantile(lo)
    upper_limit = numeric_series.quantile(hi)

    winsorized = numeric_series.clip(lower=lower_limit, upper=upper_limit)
    return winsorized


def add_outlier_flag(dataframe:pd.DataFrame,col:str,*,k: float = 1.5) -> pd.DataFrame:
    lower, upper = iqr_bounds(dataframe[col], k=k)

    is_outlier = (dataframe[col] < lower) | (dataframe[col] > upper)
    flag_col = f"{col}__outlier"

    return dataframe.assign(**{flag_col:is_outlier})