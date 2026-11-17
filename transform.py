"""Transform: clean the validated rows and derive time columns for easy dashboard queries."""
import config  # noqa: F401  (must be first)

import pandas as pd
from prefect import task

from config import get_logger


### UTILITIES
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows whose id was non-numeric (flagged NaN during validation)."""
    return df.dropna(subset=["id"])


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """One row per id. If an id appears twice (e.g. incremental + backfill overlap), keep the most recently updated."""
    return df.sort_values("last_updated").drop_duplicates(subset=["id"], keep="last")


def split_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Parse timestamps and add year/month/day/hour/minute/second columns."""
    df["date_time"] = pd.to_datetime(df["date_time"])
    df["last_updated"] = pd.to_datetime(df["last_updated"])
    df["year"] = df["date_time"].dt.year
    df["month"] = df["date_time"].dt.month
    df["day"] = df["date_time"].dt.day
    df["hour"] = df["date_time"].dt.hour
    df["minute"] = df["date_time"].dt.minute
    df["second"] = df["date_time"].dt.second
    return df


### TRANSFORMATION LOGIC
@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Drop invalid ids, deduplicate by id (latest wins), split datetime, and set column types for loading."""
    df = df.copy()
    n_in = len(df)
    df = remove_invalid_rows(df)
    # last_updated is a string at this point; ISO strings sort correctly, but convert first to be safe
    df["last_updated"] = pd.to_datetime(df["last_updated"])
    df = remove_duplicates(df)
    df = split_datetime(df)
    df["id"] = df["id"].astype("int64")
    # NaN -> None so psycopg2 writes SQL NULL
    df = df.astype(object).where(df.notna(), None)
    get_logger().info("Transform: %d rows in, %d rows out.", n_in, len(df))
    return df
