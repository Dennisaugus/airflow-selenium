"""Helper functions to clean DataFrame columns"""
from pandas.api.types import is_string_dtype
import pandas as pd


def clean_monetary_columns(
    df_origin: pd.DataFrame, column_name_list: list
) -> pd.DataFrame:
    """String regex operations to clean columns with monetary values"""
    df = df_origin.copy()
    df.replace({"R\$": ""}, regex=True, inplace=True)

    for column_name in column_name_list:
        if is_string_dtype(df[column_name]):
            df[column_name] = df[column_name].str.strip()
            df[column_name] = df[column_name].str.replace(".", "", regex=True)
            df[column_name] = df[column_name].str.replace(",", ".", regex=True)
            df[column_name] = df[column_name].str.replace(r"^-$", "0", regex=True)
            df[column_name] = df[column_name].str.replace(R"\(", "-", regex=True)
            df[column_name] = df[column_name].str.replace(R"\)", "", regex=True)
            df[column_name] = df[column_name].astype(float)

    return df


def clean_pct_columns(df_origin: pd.DataFrame, columns_to_clean: list) -> pd.DataFrame:
    """String regex operations to clean columns with percentage values"""
    df = df_origin.copy()
    for column_name in columns_to_clean:
        if is_string_dtype(df[column_name]):
            df[column_name] = df[column_name].str.strip()
            df[column_name] = df[column_name].str.replace(r"\%", "", regex=True)
            df[column_name] = df[column_name].str.replace(r"^-$", "0", regex=True)
            df[column_name] = df[column_name].str.replace(r",", ".", regex=True)
            df[column_name] = df[column_name].astype(float)
            df[column_name] = df[column_name] / 100

    return df


def fill_zeros(x: str, length: int) -> str:
    """Given a string input fill with leading zeros"""
    return x.zfill(length) if len(x) < length else x


def count_dropped_rows_with_errors(df: pd.DataFrame) -> pd.DataFrame:
    """Search for #REF! on the DataFrame and drop those rows.

    Args:
        df (pd.DataFrame): input DataFrame

    Returns:
        pd.DataFrame: filtered DataFrame
    """

    df_copy = df.copy()

    rows = len(df_copy)
    df_copy = df_copy.drop(
        df_copy[
            (df_copy == "#REF!").any(axis=1) | (df_copy == "#DIV/0!").any(axis=1)
        ].index
    )
    rows_after_drop = len(df_copy)

    print(
        f"Dropped rows with missing ref (or other errors) count: {rows-rows_after_drop}"
    )

    return df_copy


def count_dropped_na_rows(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    """Drop rows where NA values are found within a column subset or the entire df.

    Args:
        df (pd.DataFrame): Input DataFrame
        subset (list, optional): Columns to use as subset filter. Defaults to None.

    Returns:
        pd.DataFrame: Output DataFrame
    """
    df_copy = df.copy()

    rows = len(df_copy)
    df_copy.dropna(subset=subset, inplace=True)
    rows_after_drop = len(df_copy)

    print(f"Dropped rows with nulls count: {rows-rows_after_drop}")

    return df_copy
