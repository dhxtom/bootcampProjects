import pandas as pd


def validate_required_columns(dataframe: pd.DataFrame, required_columns: list[str]) -> None:
   # check in dataFrame column
    existing_columns = set(dataframe.columns)
    missing_columns = []
    
    for column in required_columns:
        if column not in existing_columns:
            missing_columns.append(column)
    
    if missing_columns:
        raise ValueError(f" missing columns : {missing_columns}")


def validate_not_empty(dataframe: pd.DataFrame, dataframe_name: str = "dataframe") -> None:
 # is not empty
    if dataframe.empty:
        raise ValueError(f"{dataframe_name} no row")


def validate_unique_column(
        
    dataframe: pd.DataFrame,
    column_name: str,
    allow_missing: bool = False,) -> None:
  # Check for duplicates
   
    if column_name not in dataframe.columns:
        raise ValueError(f"columns '{column_name}'not in dataframe")
    
    column_values = dataframe[column_name]
    
    # check missing value 
    if not allow_missing:
        missing_count = column_values.isna().sum()
        if missing_count > 0:
            raise ValueError(f"column name '{column_name}  {missing_count} missing value ")
    
    duplicated_rows = column_values.duplicated(keep=False) & column_values.notna()
    duplicated_count = duplicated_rows.sum()
    
    if duplicated_count > 0:
        duplicated_values = column_values[duplicated_rows].unique()
        raise ValueError(
            f"raw '{column_name} {duplicated_count} \n"
            f"duplicate values : {list(duplicated_values)}")
    
    

def assert_in_range(
    # Ensure values do not exceed minimum or maximum limits
    series: pd.Series,
    min_value: float | None = None,
    max_value: float | None = None,
    value_name: str = "value",
) -> None:
    clean_values = series.dropna()

    if min_value is not None:
        below_min = (clean_values < min_value).any()
        if below_min:
            raise ValueError(f"{value_name} has values below {min_value}")

    if max_value is not None:
        above_max = (clean_values > max_value).any()
        if above_max:
            raise ValueError(f"{value_name} has values above {max_value}")

    

    
