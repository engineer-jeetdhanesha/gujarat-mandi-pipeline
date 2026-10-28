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
        ({"t_market": ""}, ["missing_market"]),    # empty market fails
        ({"t_market": None}, ["missing_market"]),  # null market fails
        ({}, []),                                  # valid row passes
    ],
)
def test__dq_check__missing_market(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__missing_market(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Market is missing"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"t_commodity_code": ""}, ["missing_commodity_code"]),    # empty code fails
        ({"t_commodity_code": None}, ["missing_commodity_code"]),  # null code fails
        ({}, []),                                                  # valid row passes
    ],
)
def test__dq_check__missing_commodity_code(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__missing_commodity_code(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Commodity code is missing"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"d_date": None}, ["invalid_date"]),  # date could not be parsed: fails
        ({}, []),                              # valid date passes
        ({"d_date": date(2999, 1, 1)}, []),    # a future date is valid here (future_date handles it)
    ],
)
def test__dq_check__invalid_date(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__invalid_date(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Arrival date is missing or not dd/MM/yyyy"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"d_min": None}, ["invalid_price"]),  # min price is not a number: fails
        ({"d_max": None}, ["invalid_price"]),  # max price is not a number: fails
        ({}, []),                              # valid prices pass
    ],
)
def test__dq_check__invalid_price(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__invalid_price(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["A price is missing or not a number"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"d_min": Decimal("0")}, ["price_le_zero"]),                    # a zero price fails
        ({"d_min": None, "d_modal": Decimal("-1")}, []),                 # skipped: a price is invalid, invalid_price reports it
        ({}, []),                                                        # valid row passes
    ],
)
def test__dq_check__price_le_zero(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__price_le_zero(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["A price is zero or negative"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"d_min": Decimal("40")}, ["min_gt_max"]),              # min 40 is above max 30: fails
        ({"d_min": Decimal("40"), "d_modal": None}, []),         # skipped: a price is invalid
        ({}, []),                                                # valid row passes
    ],
)
def test__dq_check__min_gt_max(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__min_gt_max(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Min price is greater than max price"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"d_modal": Decimal("99")}, ["modal_out_of_range"]),             # modal 99 is above max 30: fails
        ({"d_modal": Decimal("5")}, ["modal_out_of_range"]),              # modal 5 is below min 10: fails
        ({"d_min": Decimal("40"), "d_modal": Decimal("99")}, []),         # skipped: min is above max, min_gt_max reports it
    ],
)
def test__dq_check__modal_out_of_range(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__modal_out_of_range(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Modal price is outside the min-max range"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"dm_market_key": None}, ["unknown_market"]),            # market not found in dim_market: fails
        ({"t_market": "", "dm_market_key": None}, []),            # blank market: missing_market reports it, not this rule
        ({}, []),                                                 # known market passes
    ],
)
def test__dq_check__unknown_market(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__unknown_market(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Market is not in dim_market"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"dc_commodity_key": None}, ["unknown_commodity"]),         # code not found in dim_commodity: fails
        ({"t_commodity_code": "", "dc_commodity_key": None}, []),    # blank code: missing_commodity_code reports it
        ({}, []),                                                    # known commodity passes
    ],
)
def test__dq_check__unknown_commodity(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__unknown_commodity(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Commodity code is not in dim_commodity"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"d_date": date(2999, 1, 1)}, ["future_date"]),  # date is after today: fails
        ({"d_date": None}, []),                           # skipped: date is invalid, invalid_date reports it
        ({}, []),                                         # past date passes
    ],
)
def test__dq_check__future_date(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__future_date(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Arrival date is in the future"] if rule_ids else [])


@pytest.mark.parametrize(
    "changes, rule_ids",
    [
        ({"dd_date_key": None}, ["unknown_date"]),           # date not found in dim_date: fails
        ({"d_date": None, "dd_date_key": None}, []),         # skipped: date is invalid, invalid_date reports it
        ({}, []),                                            # known date passes
    ],
)
def test__dq_check__unknown_date(spark: SparkSession, changes: dict, rule_ids: list[str]) -> None:
    df = spark.createDataFrame([{**PASSING_ROW, **changes}], ROW_SCHEMA)

    row = dq.dq_check__unknown_date(df).first()

    assert row["rule_ids"] == rule_ids
    assert row["rule_reasons"] == (["Arrival date is not in dim_date"] if rule_ids else [])


@pytest.mark.parametrize(
    "rows, expected",
    [
        # (row_no, ingested_at, rule_ids already set) -> rule_ids per row_no afterwards
        ([(1, datetime(2026, 1, 6), []), (2, datetime(2026, 1, 7), [])], {1: ["duplicate"], 2: []}),  # latest load wins
        ([(1, datetime(2026, 1, 7), []), (2, datetime(2026, 1, 7), [])], {1: ["duplicate"], 2: []}),  # same load: highest row number wins
        ([(1, datetime(2026, 1, 7), []), (2, datetime(2026, 1, 8), ["invalid_price"])], {1: [], 2: ["invalid_price"]}),  # a row that already failed is ignored
    ],
)
def test__dq_check__duplicate(spark: SparkSession, rows: list[tuple], expected: dict[int, list[str]]) -> None:
    # All rows share one natural key (market, commodity code, variety, grade, date).
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
        (
            [("1", "Gujarat", "Ahmedabad", "M1"), ("abc", "Gujarat", "Surat", "M2"), ("x", "Gujarat", "Surat", "M3")],
            ["2 value(s) of 'market_key' are not a valid bigint"],
        ),
        ([("1", "Gujarat", "Ahmedabad", "M1")], []),  # every value fits
        ([(None, "Gujarat", "Ahmedabad", "M1")], []),  # an empty value is not a cast problem (the not-null check reports it)
    ],
)
def test__dq_check__dim_cast(spark: SparkSession, rows: list[tuple], problems: list[str]) -> None:
    df = spark.createDataFrame(rows, MARKET_STRINGS)

    assert dq.dq_check__dim_cast(df, SCHEMA_DIM_MARKET) == problems


@pytest.mark.parametrize(
    "rows, problems",
    [
        ([(None, "Gujarat", "Ahmedabad", "M1")], ["1 row(s) have an empty 'market_key'"]),  # empty key
        (
            [("2", "Gujarat", "Surat", None), ("3", "Gujarat", "Rajkot", None)],
            ["2 row(s) have an empty 'market_name'"],  # two empty names
        ),
        ([("1", "Gujarat", "Ahmedabad", "M1")], []),  # everything filled
    ],
)
def test__dq_check__dim_not_null(spark: SparkSession, rows: list[tuple], problems: list[str]) -> None:
    df = spark.createDataFrame(rows, MARKET_STRINGS)

    assert dq.dq_check__dim_not_null(df, ["market_key", "district_name", "market_name"]) == problems


@pytest.mark.parametrize(
    "rows, columns, problems",
    [
        # key 1 appears twice
        (
            [("1", "Gujarat", "Ahmedabad", "M1"), ("1", "Gujarat", "Surat", "M2")],
            ["market_key"],
            ["1 duplicate value(s) of (market_key), for example [('1',)]"],
        ),
        # key 1 appears twice, but key and name together are unique
        ([("1", "Gujarat", "Ahmedabad", "M1"), ("1", "Gujarat", "Surat", "M2")], ["market_key", "market_name"], []),
        ([("1", "Gujarat", "Ahmedabad", "M1"), ("2", "Gujarat", "Surat", "M2")], ["market_key"], []),  # all keys differ
    ],
)
def test__dq_check__dim_unique(spark: SparkSession, rows: list[tuple], columns: list[str], problems: list[str]) -> None:
    df = spark.createDataFrame(rows, MARKET_STRINGS)

    assert dq.dq_check__dim_unique(df, columns) == problems


@pytest.mark.parametrize(
    "days, problems",
    [
        ([date(2026, 1, 1), date(2026, 1, 4)], ["2 day(s) are missing between 2026-01-01 and 2026-01-04 in 'date'"]),  # 2 and 3 Jan missing
        ([date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)], []),  # every day is there
        ([], []),                                                      # no rows, nothing to check
    ],
)
def test__dq_check__dim_date_gaps(spark: SparkSession, days: list[date], problems: list[str]) -> None:
    df = spark.createDataFrame([(day,) for day in days], "date date")

    assert dq.dq_check__dim_date_gaps(df, "date") == problems
