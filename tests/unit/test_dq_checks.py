"""Tests for lib_mandi.dq_checks: one test per row check and per dimension check."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from lib_mandi import dq_checks as dq
from lib_mandi.config import SCHEMA_DIM_MARKET

# --- silver transactions: one test per rule, three cases each ---

# Columns the row checks read (see the top of lib_mandi/dq_checks.py).
# rule_ids and rule_reasons start empty; a check adds to them only when the row fails.
ROW_SCHEMA = (
    "t_market string, t_commodity_code string, d_date date, "
    "d_min decimal(10,2), d_max decimal(10,2), d_modal decimal(10,2), "
    "dm_market_key bigint, dc_commodity_key bigint, dd_date_key int, "
    "rule_ids array<string>, rule_reasons array<string>"
)

# A row that passes every rule. Each case changes only the columns it is about.
PASSING_ROW = {
    "t_market": "M1", "t_commodity_code": "100", "d_date": date(2026, 1, 5),
    "d_min": Decimal("10"), "d_max": Decimal("30"), "d_modal": Decimal("20"),
    "dm_market_key": 1, "dc_commodity_key": 10, "dd_date_key": 20260105,
    "rule_ids": [], "rule_reasons": [],
}


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"t_market": ""}, ["missing_market"], id="empty_market_fails"),  # empty market fails
        pytest.param({"t_market": None}, ["missing_market"], id="null_market_fails"),  # null market fails
        pytest.param({}, [], id="valid_row_passes"),  # valid row passes
    ],
)
def test__dq_check__missing_market(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """An empty or null market is flagged.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__missing_market(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Market is missing"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"t_commodity_code": ""}, ["missing_commodity_code"], id="empty_code_fails"),  # empty code fails
        pytest.param({"t_commodity_code": None}, ["missing_commodity_code"], id="null_code_fails"),  # null code fails
        pytest.param({}, [], id="valid_row_passes"),  # valid row passes
    ],
)
def test__dq_check__missing_commodity_code(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """An empty or null commodity code is flagged.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__missing_commodity_code(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Commodity code is missing"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"d_date": None}, ["invalid_date"], id="unparsable_date_fails"),  # date could not be parsed: fails
        pytest.param({}, [], id="valid_date_passes"),  # valid date passes
        pytest.param({"d_date": date(2999, 1, 1)}, [], id="future_date_passes"),  # a future date is valid here (future_date handles it)
    ],
)
def test__dq_check__invalid_date(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """A date that could not be parsed is flagged; a future date is not.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__invalid_date(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Arrival date is missing or not dd/MM/yyyy"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"d_min": None}, ["invalid_price"], id="bad_min_price_fails"),  # min price is not a number: fails
        pytest.param({"d_max": None}, ["invalid_price"], id="bad_max_price_fails"),  # max price is not a number: fails
        pytest.param({}, [], id="valid_prices_pass"),  # valid prices pass
    ],
)
def test__dq_check__invalid_price(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """A min or max price that is not a number is flagged.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__invalid_price(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["A price is missing or not a number"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"d_min": Decimal("0")}, ["price_le_zero"], id="zero_price_fails"),  # a zero price fails
        pytest.param({"d_min": None, "d_modal": Decimal("-1")}, [], id="skipped_when_price_invalid"),  # skipped: a price is invalid, invalid_price reports it
        pytest.param({}, [], id="valid_row_passes"),  # valid row passes
    ],
)
def test__dq_check__price_le_zero(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """A zero or negative price is flagged; skipped if a price is invalid.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__price_le_zero(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["A price is zero or negative"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"d_min": Decimal("40")}, ["min_gt_max"], id="min_above_max_fails"),  # min 40 is above max 30: fails
        pytest.param({"d_min": Decimal("40"), "d_modal": None}, [], id="skipped_when_price_invalid"),  # skipped: a price is invalid
        pytest.param({}, [], id="valid_row_passes"),  # valid row passes
    ],
)
def test__dq_check__min_gt_max(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """Min price above max price is flagged; skipped if a price is invalid.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__min_gt_max(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Min price is greater than max price"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"d_modal": Decimal("99")}, ["modal_out_of_range"], id="modal_above_max_fails"),  # modal 99 is above max 30: fails
        pytest.param({"d_modal": Decimal("5")}, ["modal_out_of_range"], id="modal_below_min_fails"),  # modal 5 is below min 10: fails
        pytest.param({"d_min": Decimal("40"), "d_modal": Decimal("99")}, [], id="skipped_when_min_above_max"),  # skipped: min is above max, min_gt_max reports it
    ],
)
def test__dq_check__modal_out_of_range(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """Modal price outside min-max is flagged; skipped if min > max.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__modal_out_of_range(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Modal price is outside the min-max range"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"dm_market_key": None}, ["unknown_market"], id="unknown_market_fails"),  # market not found in dim_market: fails
        pytest.param({"t_market": "", "dm_market_key": None}, [], id="blank_market_skipped"),  # blank market: missing_market reports it, not this rule
        pytest.param({}, [], id="known_market_passes"),  # known market passes
    ],
)
def test__dq_check__unknown_market(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """A market missing from dim_market is flagged; a blank one is not.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__unknown_market(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Market is not in dim_market"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"dc_commodity_key": None}, ["unknown_commodity"], id="unknown_code_fails"),  # code not found in dim_commodity: fails
        pytest.param({"t_commodity_code": "", "dc_commodity_key": None}, [], id="blank_code_skipped"),  # blank code: missing_commodity_code reports it
        pytest.param({}, [], id="known_commodity_passes"),  # known commodity passes
    ],
)
def test__dq_check__unknown_commodity(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """A commodity code missing from dim_commodity is flagged; a blank one is not.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__unknown_commodity(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Commodity code is not in dim_commodity"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"d_date": date(2999, 1, 1)}, ["future_date"], id="future_date_fails"),  # date is after today: fails
        pytest.param({"d_date": None}, [], id="skipped_when_date_invalid"),  # skipped: date is invalid, invalid_date reports it
        pytest.param({}, [], id="past_date_passes"),  # past date passes
    ],
)
def test__dq_check__future_date(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """A date after today is flagged; skipped if the date is invalid.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__future_date(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Arrival date is in the future"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        pytest.param({"dd_date_key": None}, ["unknown_date"], id="unknown_date_fails"),  # date not found in dim_date: fails
        pytest.param({"d_date": None, "dd_date_key": None}, [], id="skipped_when_date_invalid"),  # skipped: date is invalid, invalid_date reports it
        pytest.param({}, [], id="known_date_passes"),  # known date passes
    ],
)
def test__dq_check__unknown_date(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    """A date missing from dim_date is flagged; skipped if the date is invalid.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Column overrides on a valid row ({} = valid row).
        rule_ids: Rule ids expected on the row (empty = row passes).

    Returns:
        None.
    """
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__unknown_date(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Arrival date is not in dim_date"] if rule_ids else [])


@pytest.mark.parametrize(
    "rows, expected",
    [
        # (row_no, ingested_at, rule_ids already set) -> rule_ids per row_no afterwards
        pytest.param([(1, datetime(2026, 1, 6), []), (2, datetime(2026, 1, 7), [])], {1: ["duplicate"], 2: []}, id="latest_load_wins"),  # latest load wins
        pytest.param([(1, datetime(2026, 1, 7), []), (2, datetime(2026, 1, 7), [])], {1: ["duplicate"], 2: []}, id="same_load_highest_row_wins"),  # same load: highest row number wins
        pytest.param([(1, datetime(2026, 1, 7), []), (2, datetime(2026, 1, 8), ["invalid_price"])], {1: [], 2: ["invalid_price"]}, id="failed_row_ignored"),  # a row that already failed is ignored
    ],
)
def test__dq_check__duplicate(spark: SparkSession, rows: list[tuple], expected: dict[int, list[str]]) -> None:
    """Rows with one natural key: latest load wins, then highest row number.

    Args:
        spark: Local Spark session with Delta (fixture).
        rows: (row number, ingested_at, rule ids already set) per row.
        expected: Row number -> rule ids expected afterwards.

    Returns:
        None.
    """
    df = spark.createDataFrame(rows, "_row_no long, _ingested_at timestamp, rule_ids array<string>").selectExpr(
        "*", "'M1' AS t_market", "100 AS d_code", "'Other' AS t_variety", "'FAQ' AS t_grade",
        "DATE'2026-01-05' AS d_date", "CAST(array() AS array<string>) AS rule_reasons",
    )

    result = {row["_row_no"]: row["rule_ids"] for row in dq.dq_check__duplicate(df).collect()}

    assert result == expected


# --- silver dimensions: one test per check, three cases each ---

# Bronze layout of dim_market: every value is a string.
MARKET_STRINGS = "market_key string, state string, district_name string, market_name string"


@pytest.mark.parametrize(
    "rows, problems",
    [
        # two of three keys cannot be cast to bigint
        pytest.param(
            [("1", "Gujarat", "Ahmedabad", "M1"), ("abc", "Gujarat", "Surat", "M2"), ("x", "Gujarat", "Surat", "M3")],
            ["2 value(s) of 'market_key' are not a valid bigint"],
            id="bad_keys_reported",
        ),
        pytest.param([("1", "Gujarat", "Ahmedabad", "M1")], [], id="all_values_fit"),  # every value fits
        pytest.param([(None, "Gujarat", "Ahmedabad", "M1")], [], id="empty_value_not_a_cast_problem"),  # an empty value is not a cast problem (the not-null check reports it)
    ],
)
def test__dq_check__dim_cast(spark: SparkSession, rows: list[tuple], problems: list[str]) -> None:
    """Values that cannot be cast to the silver type are counted per column.

    Args:
        spark: Local Spark session with Delta (fixture).
        rows: dim_market bronze rows, all strings.
        problems: Messages expected (empty = all fit).

    Returns:
        None.
    """
    df = spark.createDataFrame(rows, MARKET_STRINGS)

    assert dq.dq_check__dim_cast(df, SCHEMA_DIM_MARKET) == problems


@pytest.mark.parametrize(
    "rows, problems",
    [
        pytest.param([(None, "Gujarat", "Ahmedabad", "M1")], ["1 row(s) have an empty 'market_key'"], id="empty_key_reported"),  # empty key
        pytest.param(
            [("2", "Gujarat", "Surat", None), ("3", "Gujarat", "Rajkot", None)],
            ["2 row(s) have an empty 'market_name'"],  # two empty names
            id="two_empty_names_reported",
        ),
        pytest.param([("1", "Gujarat", "Ahmedabad", "M1")], [], id="all_filled"),  # everything filled
    ],
)
def test__dq_check__dim_not_null(spark: SparkSession, rows: list[tuple], problems: list[str]) -> None:
    """Empty values in required columns are counted per column.

    Args:
        spark: Local Spark session with Delta (fixture).
        rows: dim_market bronze rows, all strings.
        problems: Messages expected (empty = all filled).

    Returns:
        None.
    """
    df = spark.createDataFrame(rows, MARKET_STRINGS)

    assert dq.dq_check__dim_not_null(df, ["market_key", "district_name", "market_name"]) == problems


@pytest.mark.parametrize(
    "rows, columns, problems",
    [
        # key 1 appears twice
        pytest.param(
            [("1", "Gujarat", "Ahmedabad", "M1"), ("1", "Gujarat", "Surat", "M2")],
            ["market_key"],
            ["1 duplicate value(s) of (market_key), for example [('1',)]"],
            id="duplicate_key_reported",
        ),
        # key 1 appears twice, but key and name together are unique
        pytest.param([("1", "Gujarat", "Ahmedabad", "M1"), ("1", "Gujarat", "Surat", "M2")], ["market_key", "market_name"], [], id="unique_as_a_pair"),
        pytest.param([("1", "Gujarat", "Ahmedabad", "M1"), ("2", "Gujarat", "Surat", "M2")], ["market_key"], [], id="all_keys_differ"),  # all keys differ
    ],
)
def test__dq_check__dim_unique(spark: SparkSession, rows: list[tuple], columns: list[str], problems: list[str]) -> None:
    """Repeated values of the column combination are reported.

    Args:
        spark: Local Spark session with Delta (fixture).
        rows: dim_market bronze rows, all strings.
        columns: Columns that must be unique together.
        problems: Messages expected (empty = unique).

    Returns:
        None.
    """
    df = spark.createDataFrame(rows, MARKET_STRINGS)

    assert dq.dq_check__dim_unique(df, columns) == problems


@pytest.mark.parametrize(
    "days, problems",
    [
        pytest.param([date(2026, 1, 1), date(2026, 1, 4)], ["2 day(s) are missing between 2026-01-01 and 2026-01-04 in 'date'"], id="gap_reported"),  # 2 and 3 Jan missing
        pytest.param([date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)], [], id="no_gap"),  # every day is there
        pytest.param([], [], id="no_rows"),  # no rows, nothing to check
    ],
)
def test__dq_check__dim_date_gaps(spark: SparkSession, days: list[date], problems: list[str]) -> None:
    """Missing days between the first and last day are reported.

    Args:
        spark: Local Spark session with Delta (fixture).
        days: Dates in the dim_date table.
        problems: Messages expected (empty = no gaps).

    Returns:
        None.
    """
    df = spark.createDataFrame([(day,) for day in days], "date date")

    assert dq.dq_check__dim_date_gaps(df, "date") == problems
