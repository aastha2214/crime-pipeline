"""Offline tests (no network, no database) for the validation / transform / safety rules."""
import pandas as pd
import pytest

from config import EXPECTED_COLUMNS
from load import LoadError, _check_delete_safety
from transform import transform_data
from validate import (DataQualityError, SchemaChangeError, check_source_schema, describe_schema_change,
                      diff_schema, validate_data)


def make_df(**overrides):
    row = {"date_time": "2026-01-02T03:04:05.000", "id": "100", "type": "INCIDENT", "subtype": "X",
           "location": "MAIN ST", "last_updated": "2026-01-03T00:00:00.000", "description": "d"}
    row.update(overrides)
    return pd.DataFrame([row])


def test_schema_ok():
    check_source_schema(EXPECTED_COLUMNS)


def test_schema_added_removed_and_rename_are_reported():
    cols = [c for c in EXPECTED_COLUMNS if c != "location"] + ["new_col", "location_name"]
    added, removed = diff_schema(cols)
    assert added == ["location_name", "new_col"] and removed == ["location"]
    text = describe_schema_change(added, removed)
    assert "removed columns: ['location']" in text and "'location' -> 'location_name'" in text
    with pytest.raises(SchemaChangeError, match="location_name"):
        check_source_schema(cols)


def test_unexpected_column_in_rows_fails():
    with pytest.raises(SchemaChangeError, match="surprise"):
        validate_data.fn(make_df(surprise=1), EXPECTED_COLUMNS)


def test_optional_column_omitted_by_socrata_is_fine():
    out = validate_data.fn(make_df().drop(columns=["subtype"]), EXPECTED_COLUMNS)
    assert out["subtype"].isna().all()


def test_missing_required_value_fails():
    with pytest.raises(DataQualityError, match="'location'"):
        validate_data.fn(make_df(location=None), EXPECTED_COLUMNS)


def test_bad_datetime_fails():
    with pytest.raises(DataQualityError, match="ISO 8601"):
        validate_data.fn(make_df(date_time="01/02/2026"), EXPECTED_COLUMNS)


def test_non_numeric_id_is_dropped_not_fatal():
    df = pd.concat([make_df(id="abc"), make_df(id="7")], ignore_index=True)
    out = transform_data.fn(validate_data.fn(df, EXPECTED_COLUMNS))
    assert out["id"].tolist() == [7]


def test_transform_dedupes_keeping_latest_and_splits_datetime():
    df = pd.concat([make_df(description="old", last_updated="2026-01-03T00:00:00.000"),
                    make_df(description="new", last_updated="2026-02-03T00:00:00.000")], ignore_index=True)
    out = transform_data.fn(validate_data.fn(df, EXPECTED_COLUMNS))
    assert len(out) == 1 and out.iloc[0]["description"] == "new"
    assert (out.iloc[0]["year"], out.iloc[0]["month"], out.iloc[0]["hour"]) == (2026, 1, 3)


def test_delete_safety_blocks_truncated_source():
    _check_delete_safety(n_source=500, n_gone=2, n_active=502)  # normal: fine
    with pytest.raises(LoadError):
        _check_delete_safety(n_source=0, n_gone=0, n_active=100)
    with pytest.raises(LoadError):
        _check_delete_safety(n_source=40, n_gone=60, n_active=100)
