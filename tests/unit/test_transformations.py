"""Tests for lib_mandi.transformations: bronze, silver, fact, gold and dimension logic."""

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from lib_mandi import transformations as t
from lib_mandi.config import (
    RULES_DIM_DATE,
    RULES_DIM_MARKET,
    SCHEMA_DIM_DATE,
    SCHEMA_DIM_MARKET,
    SCHEMA_SOURCE,
)

# --- bronze ---

def test__bronze_adds_metadata_and_numbers_the_rows_in_file_order(spark: SparkSession) -> None:
    """Adds run id, file name, source mode, time and row numbers from 1.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    df = spark.createDataFrame([("Gujarat",) + ("x",) * 10, ("Gujarat",) + ("y",) * 10], SCHEMA_SOURCE)

    rows = t.transform__bronze(df, "run1", "f.csv").orderBy("_row_no").collect()

    assert [row["_row_no"] for row in rows] == [1, 2]
    assert [row["district"] for row in rows] == ["x", "y"]
    assert {(row["_run_id"], row["_file_name"], row["_source_mode"]) for row in rows} == {("run1", "f.csv", "FILE")}
    assert all(row["_ingested_at"] is not None for row in rows)


# --- silver: clean and rejected rows ---

# Bronze row layout: the 11 source columns (all strings) and the 5 columns bronze adds.
BRONZE_SCHEMA = (
    "state string, district string, market string, commodity string, commodity_code string, variety string, "
    "grade string, arrival_date string, min_price string, max_price string, modal_price string, "
    "_source_mode string, _file_name string, _run_id string, _ingested_at timestamp, _row_no long"
)

# A bronze row that passes every rule. Each test changes only the columns it is about.
VALID_ROW = {
    "state": "Gujarat", "district": "Ahmedabad", "market": "M1", "commodity": "Wheat", "commodity_code": "100",
    "variety": "Other", "grade": "FAQ", "arrival_date": "05/01/2026",
    "min_price": "10", "max_price": "30", "modal_price": "20",
    "_source_mode": "FILE", "_file_name": "f.csv", "_run_id": "run1", "_ingested_at": datetime(2026, 1, 6), "_row_no": 1,
}


def test__valid_row_goes_to_clean_with_trimmed_values(spark: SparkSession) -> None:
    """A valid row goes to clean with trimmed, typed values.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    bronze = spark.createDataFrame([{**VALID_ROW, "market": "  M1 ", "variety": " Other "}], BRONZE_SCHEMA)
    # The dimensions the row is looked up in: one market, one commodity, one date.
    market = spark.createDataFrame([(1, "M1", "Ahmedabad")], ["market_key", "market_name", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat")], ["commodity_key", "commodity_code", "commodity_name"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    clean, rejected = t.transform__silver_transactions(bronze, market, commodity, dt, "run1", SCHEMA_SOURCE)

    assert rejected.count() == 0
    row = clean.first()
    # Spaces are trimmed, the district comes from the dimension and the strings are typed.
    assert (row["market"], row["variety"], row["district_std"], row["commodity"]) == ("M1", "Other", "Ahmedabad", "Wheat")
    assert (row["commodity_code"], row["arrival_date"], row["run_id"]) == (100, date(2026, 1, 5), "run1")


@pytest.mark.parametrize(
    "changes, rule",
    [
        pytest.param({"market": " "}, "missing_market", id="missing_market"),
        pytest.param({"commodity_code": ""}, "missing_commodity_code", id="missing_commodity_code"),
        pytest.param({"arrival_date": "2026-01-05"}, "invalid_date", id="invalid_date"),
        pytest.param({"min_price": "abc"}, "invalid_price", id="invalid_price"),
        pytest.param({"min_price": "0", "modal_price": "20"}, "price_le_zero", id="price_le_zero"),
        pytest.param({"min_price": "40"}, "min_gt_max", id="min_gt_max"),
        pytest.param({"modal_price": "99"}, "modal_out_of_range", id="modal_out_of_range"),
        pytest.param({"market": "Nowhere"}, "unknown_market", id="unknown_market"),
        pytest.param({"commodity_code": "999"}, "unknown_commodity", id="unknown_commodity"),
        pytest.param({"arrival_date": "05/01/2999"}, "future_date,unknown_date", id="future_and_unknown_date"),  # a future date is not in dim_date either
        pytest.param({"arrival_date": "06/01/2026"}, "unknown_date", id="unknown_date"),
    ],
)
def test__a_rule_sends_the_row_to_rejected(spark: SparkSession, changes: dict[str, str], rule: str) -> None:
    """A row breaking one rule goes to rejected with that rule.

    Args:
        spark: Local Spark session with Delta (fixture).
        changes: Bronze column overrides on a valid row.
        rule: Expected reject_rule.

    Returns:
        None.
    """
    bronze = spark.createDataFrame([{**VALID_ROW, **changes}], BRONZE_SCHEMA)
    market = spark.createDataFrame([(1, "M1", "Ahmedabad")], ["market_key", "market_name", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat")], ["commodity_key", "commodity_code", "commodity_name"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    clean, rejected = t.transform__silver_transactions(bronze, market, commodity, dt, "run1", SCHEMA_SOURCE)

    # The row is not in clean; it is in rejected with the rule that failed.
    assert clean.count() == 0
    row = rejected.first()
    assert row["reject_rule"] == rule
    assert row["status"] == "ERROR"


def test__several_failed_rules_are_listed_in_order(spark: SparkSession) -> None:
    """Several failed rules are listed in check order.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    bronze = spark.createDataFrame([{**VALID_ROW, "market": "", "commodity_code": ""}], BRONZE_SCHEMA)
    market = spark.createDataFrame([(1, "M1", "Ahmedabad")], ["market_key", "market_name", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat")], ["commodity_key", "commodity_code", "commodity_name"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    clean, rejected = t.transform__silver_transactions(bronze, market, commodity, dt, "run1", SCHEMA_SOURCE)

    assert rejected.first()["reject_rule"] == "missing_market,missing_commodity_code"


def test__price_rules_that_need_a_valid_price_are_skipped(spark: SparkSession) -> None:
    """Price comparisons are skipped when a price is invalid.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # min_price is not a number and modal_price is negative: only the first problem is reported.
    bronze = spark.createDataFrame([{**VALID_ROW, "min_price": "abc", "modal_price": "-1"}], BRONZE_SCHEMA)
    market = spark.createDataFrame([(1, "M1", "Ahmedabad")], ["market_key", "market_name", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat")], ["commodity_key", "commodity_code", "commodity_name"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    clean, rejected = t.transform__silver_transactions(bronze, market, commodity, dt, "run1", SCHEMA_SOURCE)

    assert rejected.first()["reject_rule"] == "invalid_price"


def test__duplicate_keeps_the_latest_ingested_then_the_highest_row_no(spark: SparkSession) -> None:
    """Of duplicates, latest load then highest row number stays in clean.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # Three valid rows with the same natural key.
    bronze = spark.createDataFrame(
        [
            {**VALID_ROW, "_ingested_at": datetime(2026, 1, 7), "_row_no": 1},  # wins: latest load, highest row number
            {**VALID_ROW, "_ingested_at": datetime(2026, 1, 6), "_row_no": 2},  # older load
            {**VALID_ROW, "_ingested_at": datetime(2026, 1, 7), "_row_no": 0},  # same load, lower row number
        ],
        BRONZE_SCHEMA,
    )
    market = spark.createDataFrame([(1, "M1", "Ahmedabad")], ["market_key", "market_name", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat")], ["commodity_key", "commodity_code", "commodity_name"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    clean, rejected = t.transform__silver_transactions(bronze, market, commodity, dt, "run1", SCHEMA_SOURCE)

    assert clean.count() == 1
    rows = {row["row_no"]: row for row in rejected.collect()}
    assert set(rows) == {0, 2}
    assert {(row["reject_rule"], row["status"]) for row in rows.values()} == {("duplicate", "REJECTED")}


def test__an_invalid_row_does_not_win_a_duplicate(spark: SparkSession) -> None:
    """An invalid row is rejected for its error, not as a duplicate.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # Row 2 has the same key and a higher row number, but its price is invalid.
    bronze = spark.createDataFrame(
        [{**VALID_ROW, "_row_no": 1}, {**VALID_ROW, "_row_no": 2, "min_price": "abc"}], BRONZE_SCHEMA
    )
    market = spark.createDataFrame([(1, "M1", "Ahmedabad")], ["market_key", "market_name", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat")], ["commodity_key", "commodity_code", "commodity_name"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    clean, rejected = t.transform__silver_transactions(bronze, market, commodity, dt, "run1", SCHEMA_SOURCE)

    # Row 1 stays in clean; row 2 is rejected for its price, not as a duplicate.
    assert clean.count() == 1
    assert rejected.first()["reject_rule"] == "invalid_price"


def test__every_row_ends_up_in_clean_or_rejected(spark: SparkSession) -> None:
    """No row is lost: clean plus rejected equals the input.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    bronze = spark.createDataFrame(
        [{**VALID_ROW}, {**VALID_ROW, "market": "", "_row_no": 2}, {**VALID_ROW, "_row_no": 3}], BRONZE_SCHEMA
    )
    market = spark.createDataFrame([(1, "M1", "Ahmedabad")], ["market_key", "market_name", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat")], ["commodity_key", "commodity_code", "commodity_name"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    clean, rejected = t.transform__silver_transactions(bronze, market, commodity, dt, "run1", SCHEMA_SOURCE)

    # Row 1 is clean, row 2 has no market, row 3 duplicates row 1.
    assert clean.count() + rejected.count() == 3


# --- silver: fact ---

def test__fact_rows_get_the_keys_of_the_dimensions(spark: SparkSession) -> None:
    """Fact rows get market, commodity and date keys.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    clean = spark.createDataFrame(
        [("M1", 100, date(2026, 1, 5), "Other", "FAQ", Decimal("1"), Decimal("3"), Decimal("2"))],
        "market string, commodity_code int, arrival_date date, variety string, grade string, "
        "min_price decimal(10,2), max_price decimal(10,2), modal_price decimal(10,2)",
    )
    market = spark.createDataFrame([(1, "M1")], ["market_key", "market_name"])
    commodity = spark.createDataFrame([(10, 100)], ["commodity_key", "commodity_code"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    out = t.transform__silver_facts(clean, market, commodity, dt)

    assert out.columns == [
        "market_key", "commodity_key", "date_key", "variety", "grade",
        "min_price", "max_price", "modal_price", "loaded_at",
    ]
    row = out.first()
    assert (row["market_key"], row["commodity_key"], row["date_key"]) == (1, 10, 20260105)


def test__fact_returns_none_for_a_market_that_is_not_in_the_dimension(spark: SparkSession) -> None:
    """An unknown market gives None.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    clean = spark.createDataFrame(
        [("Nope", 100, date(2026, 1, 5), "Other", "FAQ", Decimal("1"), Decimal("3"), Decimal("2"))],
        "market string, commodity_code int, arrival_date date, variety string, grade string, "
        "min_price decimal(10,2), max_price decimal(10,2), modal_price decimal(10,2)",
    )
    market = spark.createDataFrame([(1, "M1")], ["market_key", "market_name"])
    commodity = spark.createDataFrame([(10, 100)], ["commodity_key", "commodity_code"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])

    assert t.transform__silver_facts(clean, market, commodity, dt) is None


# --- gold: weekly mart ---

# Mondays of three weeks in December 2026.
WEEK_1, WEEK_2, WEEK_3 = date(2026, 12, 7), date(2026, 12, 14), date(2026, 12, 21)

# Fact row layout: market_key, commodity_key, date_key, variety, grade, min, max, modal, loaded_at.
FACT_SCHEMA = (
    "market_key int, commodity_key int, date_key int, variety string, grade string, "
    "min_price decimal(10,2), max_price decimal(10,2), modal_price decimal(10,2), loaded_at timestamp"
)


def test__weekly_aggregates_and_week_on_week_change(spark: SparkSession) -> None:
    """Weekly counts, prices and week-on-week change.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # District D1 has markets 1 and 2. Week 1 has two rows, week 2 has one.
    fact = spark.createDataFrame(
        [
            (1, 10, 20261207, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
            (2, 10, 20261208, "Other", "FAQ", Decimal("12"), Decimal("34"), Decimal("40"), None),
            (1, 10, 20261214, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("45"), None),
        ],
        FACT_SCHEMA,
    )
    clean = spark.createDataFrame([("D1", 100, date(2026, 12, 14))], "district_std string, commodity_code int, arrival_date date")
    market = spark.createDataFrame([(1, "D1"), (2, "D1")], ["market_key", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat", "Cereals")], ["commodity_key", "commodity_code", "commodity_name", "category"])
    # One row per day from 1 December for 40 days, with the Monday of its week.
    days = [date(2026, 12, 1) + timedelta(days=i) for i in range(40)]
    dt = spark.createDataFrame(
        [(int(d.strftime("%Y%m%d")), d, d - timedelta(days=d.weekday())) for d in days], ["date_key", "date", "week_start"]
    )

    out = t.transform__gold_weekly_mart(clean, fact, market, commodity, dt)

    rows = {row["week_start"]: row for row in out.collect()}
    week_1, week_2 = rows[WEEK_1], rows[WEEK_2]
    # Week 1: two rows from two markets; modal prices 20 and 40 give a sum of 60 and an average of 30.
    assert (week_1["price_records"], week_1["markets_count"]) == (2, 2)
    assert (week_1["min_price"], week_1["max_price"]) == (Decimal("10.00"), Decimal("34.00"))
    assert (week_1["sum_modal_price"], week_1["avg_modal_price"]) == (Decimal("60.00"), Decimal("30.00"))
    # Nothing before week 1, so there is no change to report.
    assert week_1["prev_week_avg_price"] is None and week_1["wow_change_pct"] is None
    # Week 2 averages 45 against 30 the week before: +50 percent.
    assert week_2["prev_week_avg_price"] == Decimal("30.00")
    assert week_2["wow_change_pct"] == Decimal("50.00")
    assert week_1["year"] == 2026 and week_1["week_number"] == 50


def test__a_missing_previous_week_gives_no_change(spark: SparkSession) -> None:
    """No previous week means no week-on-week change.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # Only week 3 has data; week 2 is missing, so there is nothing to compare with.
    fact = spark.createDataFrame(
        [(1, 10, 20261221, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None)], FACT_SCHEMA
    )
    clean = spark.createDataFrame([("D1", 100, date(2026, 12, 21))], "district_std string, commodity_code int, arrival_date date")
    market = spark.createDataFrame([(1, "D1"), (2, "D1")], ["market_key", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat", "Cereals")], ["commodity_key", "commodity_code", "commodity_name", "category"])
    days = [date(2026, 12, 1) + timedelta(days=i) for i in range(40)]
    dt = spark.createDataFrame(
        [(int(d.strftime("%Y%m%d")), d, d - timedelta(days=d.weekday())) for d in days], ["date_key", "date", "week_start"]
    )

    out = t.transform__gold_weekly_mart(clean, fact, market, commodity, dt)

    row = out.where(out.week_start == WEEK_3).first()
    assert row["prev_week_avg_price"] is None and row["wow_change_pct"] is None


def test__a_week_is_complete_only_inside_the_loaded_dates(spark: SparkSession) -> None:
    """A week is complete only if all its days are loaded.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # Loaded dates run from Wednesday 9 December to Tuesday 22 December.
    fact = spark.createDataFrame(
        [
            (1, 10, 20261209, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
            (1, 10, 20261214, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
            (1, 10, 20261220, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
            (1, 10, 20261222, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
        ],
        FACT_SCHEMA,
    )
    clean = spark.createDataFrame(
        [("D1", 100, date(2026, 12, 9)), ("D1", 100, date(2026, 12, 14)), ("D1", 100, date(2026, 12, 22))],
        "district_std string, commodity_code int, arrival_date date",
    )
    market = spark.createDataFrame([(1, "D1"), (2, "D1")], ["market_key", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat", "Cereals")], ["commodity_key", "commodity_code", "commodity_name", "category"])
    days = [date(2026, 12, 1) + timedelta(days=i) for i in range(40)]
    dt = spark.createDataFrame(
        [(int(d.strftime("%Y%m%d")), d, d - timedelta(days=d.weekday())) for d in days], ["date_key", "date", "week_start"]
    )

    out = t.transform__gold_weekly_mart(clean, fact, market, commodity, dt)

    complete = {row["week_start"]: row["is_complete_week"] for row in out.collect()}
    assert complete[WEEK_1] is False  # the week starts on 7 December, before the first loaded date
    assert complete[WEEK_2] is True   # 14 to 20 December is inside the loaded dates
    assert complete[WEEK_3] is False  # the week ends on 27 December, after the last loaded date


def test__only_the_weeks_around_the_run_are_returned(spark: SparkSession) -> None:
    """Returns the touched weeks plus the weeks before and after.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # Facts exist for four weeks, but the run only touched week 2.
    fact = spark.createDataFrame(
        [
            (1, 10, 20261207, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
            (1, 10, 20261214, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
            (1, 10, 20261221, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
            (1, 10, 20261228, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None),
        ],
        FACT_SCHEMA,
    )
    clean = spark.createDataFrame([("D1", 100, date(2026, 12, 14))], "district_std string, commodity_code int, arrival_date date")
    market = spark.createDataFrame([(1, "D1"), (2, "D1")], ["market_key", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat", "Cereals")], ["commodity_key", "commodity_code", "commodity_name", "category"])
    days = [date(2026, 12, 1) + timedelta(days=i) for i in range(40)]
    dt = spark.createDataFrame(
        [(int(d.strftime("%Y%m%d")), d, d - timedelta(days=d.weekday())) for d in days], ["date_key", "date", "week_start"]
    )

    out = t.transform__gold_weekly_mart(clean, fact, market, commodity, dt)

    # Week 2 plus the week before and the week after (their change depends on it); week 4 is left alone.
    assert sorted(row["week_start"] for row in out.collect()) == [WEEK_1, WEEK_2, WEEK_3]


def test__gold_returns_none_when_the_fact_has_no_rows_for_the_weeks(spark: SparkSession) -> None:
    """No facts for the touched weeks gives None.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # The fact only has 28 December, the run touched 7 December.
    fact = spark.createDataFrame(
        [(1, 10, 20261228, "Other", "FAQ", Decimal("10"), Decimal("30"), Decimal("20"), None)], FACT_SCHEMA
    )
    clean = spark.createDataFrame([("D1", 100, date(2026, 12, 7))], "district_std string, commodity_code int, arrival_date date")
    market = spark.createDataFrame([(1, "D1"), (2, "D1")], ["market_key", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat", "Cereals")], ["commodity_key", "commodity_code", "commodity_name", "category"])
    days = [date(2026, 12, 1) + timedelta(days=i) for i in range(40)]
    dt = spark.createDataFrame(
        [(int(d.strftime("%Y%m%d")), d, d - timedelta(days=d.weekday())) for d in days], ["date_key", "date", "week_start"]
    )

    assert t.transform__gold_weekly_mart(clean, fact, market, commodity, dt) is None


# --- silver: dimensions ---

# Bronze layout of dim_market: every value is a string.
DIM_MARKET_RAW = "market_key string, state string, district_name string, market_name string"
# Bronze layout of dim_date, in the order of the seed file.
DIM_DATE_RAW = ", ".join(f"{field.name} string" for field in SCHEMA_DIM_DATE.fields)


def test__silver_dims_cast_to_the_silver_types_and_add_updated_at(spark: SparkSession) -> None:
    """Casts to silver types and adds updated_at.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    df = spark.createDataFrame([("1", "Gujarat", "Ahmedabad", "M1")], DIM_MARKET_RAW)

    out = t.transform__silver_dims(
        df, SCHEMA_DIM_MARKET, RULES_DIM_MARKET["not_null_columns"], RULES_DIM_MARKET["unique_columns"], RULES_DIM_MARKET["date_column"]
    )

    assert out.columns == ["market_key", "state", "district_name", "market_name", "updated_at"]
    assert dict(out.dtypes)["market_key"] == "bigint"
    assert out.first()["updated_at"] is not None


def test__silver_dims_refuse_a_value_that_is_not_the_right_type(spark: SparkSession) -> None:
    """A value that cannot be cast gives None.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    df = spark.createDataFrame([("abc", "Gujarat", "Ahmedabad", "M1")], DIM_MARKET_RAW)

    out = t.transform__silver_dims(
        df, SCHEMA_DIM_MARKET, RULES_DIM_MARKET["not_null_columns"], RULES_DIM_MARKET["unique_columns"], RULES_DIM_MARKET["date_column"]
    )

    assert out is None


def test__silver_dims_refuse_an_empty_key(spark: SparkSession) -> None:
    """An empty key gives None.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    df = spark.createDataFrame([(None, "Gujarat", "Ahmedabad", "M1")], DIM_MARKET_RAW)

    out = t.transform__silver_dims(
        df, SCHEMA_DIM_MARKET, RULES_DIM_MARKET["not_null_columns"], RULES_DIM_MARKET["unique_columns"], RULES_DIM_MARKET["date_column"]
    )

    assert out is None


def test__silver_dims_refuse_an_empty_name(spark: SparkSession) -> None:
    """An empty name gives None.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    df = spark.createDataFrame([("1", "Gujarat", "Ahmedabad", None)], DIM_MARKET_RAW)

    out = t.transform__silver_dims(
        df, SCHEMA_DIM_MARKET, RULES_DIM_MARKET["not_null_columns"], RULES_DIM_MARKET["unique_columns"], RULES_DIM_MARKET["date_column"]
    )

    assert out is None


def test__silver_dims_refuse_a_duplicate_key(spark: SparkSession) -> None:
    """A duplicate key gives None.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # Two different markets with the same market_key.
    df = spark.createDataFrame([("1", "Gujarat", "Ahmedabad", "M1"), ("1", "Gujarat", "Surat", "M2")], DIM_MARKET_RAW)

    out = t.transform__silver_dims(
        df, SCHEMA_DIM_MARKET, RULES_DIM_MARKET["not_null_columns"], RULES_DIM_MARKET["unique_columns"], RULES_DIM_MARKET["date_column"]
    )

    assert out is None


def test__silver_dims_refuse_a_duplicate_natural_key(spark: SparkSession) -> None:
    """A duplicate natural key gives None.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # The same state, district and market name under two keys.
    df = spark.createDataFrame([("1", "Gujarat", "Ahmedabad", "M1"), ("2", "Gujarat", "Ahmedabad", "M1")], DIM_MARKET_RAW)

    out = t.transform__silver_dims(
        df, SCHEMA_DIM_MARKET, RULES_DIM_MARKET["not_null_columns"], RULES_DIM_MARKET["unique_columns"], RULES_DIM_MARKET["date_column"]
    )

    assert out is None


def test__silver_dim_date_accepts_days_without_gaps(spark: SparkSession) -> None:
    """Consecutive days pass the gap check.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    df = spark.createDataFrame(
        [
            ("20260101", "2026-01-01", "1", "Thursday", "2025-12-29", "1", "January", "1", "2026", "false", "Rabi"),
            ("20260102", "2026-01-02", "2", "Friday", "2025-12-29", "1", "January", "1", "2026", "false", "Rabi"),
            ("20260103", "2026-01-03", "3", "Saturday", "2025-12-29", "1", "January", "1", "2026", "true", "Rabi"),
        ],
        DIM_DATE_RAW,
    )

    out = t.transform__silver_dims(
        df, SCHEMA_DIM_DATE, RULES_DIM_DATE["not_null_columns"], RULES_DIM_DATE["unique_columns"], RULES_DIM_DATE["date_column"]
    )

    assert out is not None


def test__silver_dim_date_refuses_a_missing_day(spark: SparkSession) -> None:
    """A missing day gives None.

    Args:
        spark: Local Spark session with Delta (fixture).

    Returns:
        None.
    """
    # 2 January is missing between 1 January and 3 January.
    df = spark.createDataFrame(
        [
            ("20260101", "2026-01-01", "1", "Thursday", "2025-12-29", "1", "January", "1", "2026", "false", "Rabi"),
            ("20260103", "2026-01-03", "3", "Saturday", "2025-12-29", "1", "January", "1", "2026", "true", "Rabi"),
        ],
        DIM_DATE_RAW,
    )

    out = t.transform__silver_dims(
        df, SCHEMA_DIM_DATE, RULES_DIM_DATE["not_null_columns"], RULES_DIM_DATE["unique_columns"], RULES_DIM_DATE["date_column"]
    )

    assert out is None
