"""Validate: data-quality gates. Anything that makes the data untrustworthy fails the run loudly."""
import config  # noqa: F401  (must be first)

import difflib
from datetime import datetime

import pandas as pd
from prefect import task

from config import EXPECTED_COLUMNS, REQUIRED_COLUMNS, get_logger


class SchemaChangeError(ValueError):
    """The source's columns no longer match what the pipeline expects."""


class DataQualityError(ValueError):
    """Rows violate a data-quality rule."""


### UTILITIES
def diff_schema(actual_cols, expected_cols=EXPECTED_COLUMNS):
    """Return (added, removed) column lists, relative to the expected schema."""
    added = sorted(set(actual_cols) - set(expected_cols))
    removed = sorted(set(expected_cols) - set(actual_cols))
    return added, removed


def describe_schema_change(added, removed) -> str:
    """Human-readable description of exactly what changed, including likely renames."""
    lines = []
    if removed:
        lines.append(f"removed columns: {removed}")
    if added:
        lines.append(f"added columns: {added}")
    # a removed + an added column with similar names is probably a rename
    renames = []
    for old in removed:
        match = difflib.get_close_matches(old, added, n=1, cutoff=0.6)
        if match:
            renames.append(f"{old!r} -> {match[0]!r}")
    if renames:
        lines.append(f"possible renames: {', '.join(renames)}")
    return "; ".join(lines)


def check_source_schema(source_columns):
    """Compare the columns the source publishes (dataset metadata) with the expected schema."""
    added, removed = diff_schema(source_columns)
    if added or removed:
        msg = "Source schema changed - " + describe_schema_change(added, removed)
        get_logger().error(msg)
        raise SchemaChangeError(msg + ". Update EXPECTED_COLUMNS, load.py and transform.py, then rerun.")


def check_frame_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fail on columns we don't know about; fill optional columns Socrata omitted (it drops all-null columns)."""
    extra = sorted(set(df.columns) - set(EXPECTED_COLUMNS))
    if extra:
        msg = f"Fetched rows contain unexpected columns {extra} (schema change)."
        get_logger().error(msg)
        raise SchemaChangeError(msg)
    return df.reindex(columns=EXPECTED_COLUMNS)  # absent columns become NaN; required ones are caught below


def check_missing_values(df: pd.DataFrame):
    """Each incident must have a datetime, id, type, location (and last_updated, which drives incremental loads)."""
    for col in REQUIRED_COLUMNS:
        blank = df[col].isna() | (df[col].astype(str).str.strip() == "")
        if blank.any():
            raise DataQualityError(
                f"{int(blank.sum())} rows have a missing '{col}'. Example ids: {df.loc[blank, 'id'].head(3).tolist()}"
            )


def verify_datetime(df: pd.DataFrame, col: str):
    """Every value in `col` must be ISO 8601."""
    bad = []
    for value in df[col]:
        try:
            datetime.fromisoformat(str(value))
        except ValueError:
            bad.append(value)
    if bad:
        raise DataQualityError(f"{len(bad)} '{col}' values are not ISO 8601, e.g. {bad[:3]}")


def check_numeric_id(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce ids to numbers. Non-numeric ids become NaN and are dropped in transform (they can't be keyed)."""
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    n_bad = int(df["id"].isna().sum())
    if n_bad:
        get_logger().warning("%d rows have a non-numeric id; they will be dropped in transform.", n_bad)
    return df


### VALIDATION LOGIC
@task
def validate_data(df: pd.DataFrame, source_columns) -> pd.DataFrame:
    """Run all checks, in order:
    1. source schema matches expectation (via dataset metadata)
    2. fetched rows carry no unknown columns
    3. no missing values in required columns (checked on raw values, BEFORE id coercion)
    4. date_time / last_updated follow ISO 8601
    5. ids numeric (bad ones flagged NaN, removed in transform)
    """
    log = get_logger()
    check_source_schema(source_columns)
    df = check_frame_columns(df.copy())
    if df.empty:
        log.info("No rows to validate.")
        return df
    check_missing_values(df)
    verify_datetime(df, "date_time")
    verify_datetime(df, "last_updated")
    df = check_numeric_id(df)
    log.info("Validation passed for %d rows.", len(df))
    return df
