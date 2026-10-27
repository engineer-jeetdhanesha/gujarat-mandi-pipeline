from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession

from lib_mandi import transformations as t
from lib_mandi.dq_checks import dq_check__missing_market
from lib_mandi.config import SCHEMA_SOURCE


# --- bronze ---

def test__ingest_metadata_numbers_rows_in_order(spark: SparkSession) -> None:
    df = spark.createDataFrame([("Gujarat",) + ("x",) * 10, ("Gujarat",) + ("y",) * 10], SCHEMA_SOURCE)
    rows = t.transform__bronze(df, "run1", "f.csv").orderBy("_row_no").collect()
    assert [r["_row_no"] for r in rows] == [1, 2]
    assert [r["district"] for r in rows] == ["x", "y"]
    assert {(r["_run_id"], r["_file_name"], r["_source_mode"]) for r in rows} == {("run1", "f.csv", "FILE")}
    assert all(r["_ingested_at"] is not None for r in rows)


# --- silver: clean and rejected ---

BASE = {
    "state": "Gujarat", "district": "Ahmedabad", "market": "M1", "commodity": "Wheat", "commodity_code": "100",
    "variety": "Other", "grade": "FAQ", "arrival_date": "05/01/2026",
    "min_price": "10", "max_price": "30", "modal_price": "20",
}


def silver_dims(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    market = spark.createDataFrame([(1, "M1", "Ahmedabad")], ["market_key", "market_name", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat")], ["commodity_key", "commodity_code", "commodity_name"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])
    return market, commodity, dt


def bronze_df(spark: SparkSession, rows: list[dict[str, object]]) -> DataFrame:
    """rows: list of dicts of changes to BASE, optional _ingested_at / _row_no."""
    data = []
    for i, changes in enumerate(rows, start=1):
        r = {**BASE, **changes}
        data.append(
            tuple(r[c] for c in SCHEMA_SOURCE.names)
            + ("FILE", "f.csv", "run1", r.get("_ingested_at", datetime(2026, 1, 6)), r.get("_row_no", i))
        )
    return spark.createDataFrame(
        data,
        ", ".join(f"{c} string" for c in SCHEMA_SOURCE.names)
        + ", _source_mode string, _file_name string, _run_id string, _ingested_at timestamp, _row_no long",
    )


def split(spark: SparkSession, rows: list[dict[str, object]]) -> tuple[DataFrame, DataFrame]:
    """Returns (clean, rejected) for the bronze rows."""
    result = t.transform__silver_transactions(bronze_df(spark, rows), *silver_dims(spark), "run1", SCHEMA_SOURCE)
    assert result is not None
    return result


def test__valid_row_goes_to_clean_with_trimmed_values(spark: SparkSession) -> None:
    clean, rejected = split(spark, [{"market": "  M1 ", "variety": " Other "}])
    assert rejected.count() == 0
    rows = clean.collect()
    assert len(rows) == 1
    row = rows[0]
    assert (row["market"], row["variety"], row["district_std"], row["commodity"]) == ("M1", "Other", "Ahmedabad", "Wheat")
    assert (row["commodity_code"], row["arrival_date"], row["run_id"]) == (100, date(2026, 1, 5), "run1")


@pytest.mark.parametrize(
    "changes, rule",
    [
        ({"market": " "}, "missing_market"),
        ({"commodity_code": ""}, "missing_commodity_code"),
        ({"arrival_date": "2026-01-05"}, "invalid_date"),
        ({"min_price": "abc"}, "invalid_price"),
        ({"min_price": "0", "modal_price": "20"}, "price_le_zero"),
        ({"min_price": "40"}, "min_gt_max"),
        ({"modal_price": "99"}, "modal_out_of_range"),
        ({"market": "Nowhere"}, "unknown_market"),
        ({"commodity_code": "999"}, "unknown_commodity"),
        ({"arrival_date": "05/01/2999"}, "future_date,unknown_date"),  # not in dim_date either
        ({"arrival_date": "06/01/2026"}, "unknown_date"),
    ],
)
def test__rule_rejects_row(spark: SparkSession, changes: dict[str, str], rule: str) -> None:
    rejected = split(spark, [changes])[1].collect()
    assert len(rejected) == 1
    assert rejected[0]["reject_rule"] == rule
    assert rejected[0]["status"] == "ERROR"


def test__several_rules_are_listed_in_order(spark: SparkSession) -> None:
    row = split(spark, [{"market": "", "commodity_code": ""}])[1].first()
    assert row["reject_rule"] == "missing_market,missing_commodity_code"


def test__dependent_price_rules_are_skipped(spark: SparkSession) -> None:
    row = split(spark, [{"min_price": "abc", "modal_price": "-1"}])[1].first()
    assert row["reject_rule"] == "invalid_price"


def test__duplicate_keeps_latest_ingested_then_highest_row_no(spark: SparkSession) -> None:
    clean, rejected_df = split(spark, [
        {"_ingested_at": datetime(2026, 1, 7), "_row_no": 1},
        {"_ingested_at": datetime(2026, 1, 6), "_row_no": 2},
        {"_ingested_at": datetime(2026, 1, 7), "_row_no": 0},
    ])
    rejected = {r["row_no"]: r for r in rejected_df.collect()}
    assert clean.count() == 1
    assert set(rejected) == {0, 2}
    assert all(r["reject_rule"] == "duplicate" and r["status"] == "REJECTED" for r in rejected.values())


def test__invalid_row_does_not_win_a_duplicate(spark: SparkSession) -> None:
    clean, rejected = split(spark, [{"_row_no": 1}, {"_row_no": 2, "min_price": "abc"}])
    assert clean.count() == 1
    assert rejected.first()["reject_rule"] == "invalid_price"


def test__row_counts_add_up(spark: SparkSession) -> None:
    clean, rejected = split(spark, [{}, {"market": ""}, {"_row_no": 3}])
    assert clean.count() + rejected.count() == 3


# --- silver: fact ---

def fact_dims(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    market = spark.createDataFrame([(1, "M1")], ["market_key", "market_name"])
    commodity = spark.createDataFrame([(10, 100)], ["commodity_key", "commodity_code"])
    dt = spark.createDataFrame([(20260105, date(2026, 1, 5))], ["date_key", "date"])
    return market, commodity, dt


def fact_clean_df(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    return spark.createDataFrame(
        rows, "market string, commodity_code int, arrival_date date, variety string, grade string, "
        "min_price decimal(10,2), max_price decimal(10,2), modal_price decimal(10,2)",
    )


def test__build_fact_looks_up_keys(spark: SparkSession) -> None:
    clean = fact_clean_df(spark, [("M1", 100, date(2026, 1, 5), "Other", "FAQ", Decimal("1"), Decimal("3"), Decimal("2"))])
    out = t.transform__silver_facts(clean, *fact_dims(spark))
    assert out is not None
    assert out.columns == [
        "market_key", "commodity_key", "date_key", "variety", "grade",
        "min_price", "max_price", "modal_price", "loaded_at",
    ]
    row = out.first()
    assert (row["market_key"], row["commodity_key"], row["date_key"]) == (1, 10, 20260105)


def test__unknown_market_returns_none(spark: SparkSession) -> None:
    clean = fact_clean_df(spark, [("Nope", 100, date(2026, 1, 5), "Other", "FAQ", Decimal("1"), Decimal("3"), Decimal("2"))])
    assert t.transform__silver_facts(clean, *fact_dims(spark)) is None


# --- gold: weekly mart ---

MON1, MON2, MON3 = date(2026, 12, 7), date(2026, 12, 14), date(2026, 12, 21)


def gold_dims(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    market = spark.createDataFrame([(1, "D1"), (2, "D1")], ["market_key", "district_name"])
    commodity = spark.createDataFrame([(10, 100, "Wheat", "Cereals")], ["commodity_key", "commodity_code", "commodity_name", "category"])
    days = [date(2026, 12, 1) + timedelta(days=i) for i in range(40)]
    rows = []
    for d in days:
        monday = d - timedelta(days=d.weekday())
        rows.append((int(d.strftime("%Y%m%d")), d, monday))
    dt = spark.createDataFrame(rows, ["date_key", "date", "week_start"])
    return market, commodity, dt


def gold_fact_df(spark: SparkSession, rows: list[tuple[int, date, str, str, str]]) -> DataFrame:
    # (market_key, date, min, max, modal)
    return spark.createDataFrame(
        [(m, 10, int(d.strftime("%Y%m%d")), "Other", "FAQ", Decimal(lo), Decimal(hi), Decimal(mo), None)
         for m, d, lo, hi, mo in rows],
        "market_key int, commodity_key int, date_key int, variety string, grade string, "
        "min_price decimal(10,2), max_price decimal(10,2), modal_price decimal(10,2), loaded_at timestamp",
    )


def gold_clean_df(spark: SparkSession, dates: list[date]) -> DataFrame:
    return spark.createDataFrame(
        [("D1", 100, d) for d in dates], "district_std string, commodity_code int, arrival_date date"
    )


def run_gold(
    spark: SparkSession, fact_rows: list[tuple[int, date, str, str, str]], clean_dates: list[date]
) -> DataFrame:
    market, commodity, dt = gold_dims(spark)
    out = t.transform__gold_weekly_mart(gold_clean_df(spark, clean_dates), gold_fact_df(spark, fact_rows), market, commodity, dt)
    assert out is not None
    return out


def test__weekly_aggregates_and_wow_change(spark: SparkSession) -> None:
    out = run_gold(
        spark,
        [
            (1, date(2026, 12, 7), "10", "30", "20"),
            (2, date(2026, 12, 8), "12", "34", "40"),
            (1, date(2026, 12, 14), "10", "30", "45"),
        ],
        [date(2026, 12, 14)],
    )
    rows = {r["week_start"]: r for r in out.collect()}
    w1, w2 = rows[MON1], rows[MON2]
    assert (w1["price_records"], w1["markets_count"]) == (2, 2)
    assert (w1["min_price"], w1["max_price"]) == (Decimal("10.00"), Decimal("34.00"))
    assert (w1["sum_modal_price"], w1["avg_modal_price"]) == (Decimal("60.00"), Decimal("30.00"))
    assert w1["prev_week_avg_price"] is None and w1["wow_change_pct"] is None
    assert w2["prev_week_avg_price"] == Decimal("30.00")
    assert w2["wow_change_pct"] == Decimal("50.00")
    assert w1["year"] == 2026 and w1["week_number"] == 50


def test__missing_previous_week_gives_null_change(spark: SparkSession) -> None:
    out = run_gold(spark, [(1, date(2026, 12, 21), "10", "30", "20")], [date(2026, 12, 21)])
    row = out.where(out.week_start == MON3).first()
    assert row["prev_week_avg_price"] is None and row["wow_change_pct"] is None


def test__is_complete_week_uses_fact_bounds(spark: SparkSession) -> None:
    out = run_gold(
        spark,
        [
            (1, date(2026, 12, 9), "10", "30", "20"),   # first loaded date: Wednesday
            (1, date(2026, 12, 14), "10", "30", "20"),
            (1, date(2026, 12, 20), "10", "30", "20"),  # Sunday: week 3 ends here
            (1, date(2026, 12, 22), "10", "30", "20"),  # last loaded date: Tuesday
        ],
        [date(2026, 12, 9), date(2026, 12, 14), date(2026, 12, 22)],
    )
    complete = {r["week_start"]: r["is_complete_week"] for r in out.collect()}
    assert complete[MON1] is False  # starts before the first loaded date
    assert complete[MON2] is True
    assert complete[MON3] is False  # ends after the last loaded date


def test__only_rewrite_weeks_are_returned(spark: SparkSession) -> None:
    out = run_gold(
        spark,
        [(1, d, "10", "30", "20") for d in (date(2026, 12, 7), date(2026, 12, 14), date(2026, 12, 21), date(2026, 12, 28))],
        [date(2026, 12, 14)],
    )
    assert sorted(r["week_start"] for r in out.collect()) == [MON1, MON2, MON3]


def test__gold_returns_none_without_fact_rows_for_the_weeks(spark: SparkSession) -> None:
    market, commodity, dt = gold_dims(spark)
    fact = gold_fact_df(spark, [(1, date(2026, 12, 28), "10", "30", "20")])
    clean = gold_clean_df(spark, [date(2026, 12, 7)])
    assert t.transform__gold_weekly_mart(clean, fact, market, commodity, dt) is None


# --- silver: dimensions ---

DIM_MARKET_RAW = "market_key string, state string, district_name string, market_name string"


def transform__market(spark: SparkSession, rows: list[tuple[str, str, str, str]]) -> DataFrame | None:
    from lib_mandi.config import RULES_DIM_MARKET, SCHEMA_DIM_MARKET

    return t.transform__silver_dims(
        spark.createDataFrame(rows, DIM_MARKET_RAW),
        SCHEMA_DIM_MARKET,
        RULES_DIM_MARKET["not_null_columns"],
        RULES_DIM_MARKET["unique_columns"],
        RULES_DIM_MARKET["date_column"],
    )


def test__silver_dims_casts_to_the_silver_types_and_adds_updated_at(spark: SparkSession) -> None:
    out = transform__market(spark, [("1", "Gujarat", "Ahmedabad", "M1")])
    assert out is not None
    assert out.columns == ["market_key", "state", "district_name", "market_name", "updated_at"]
    assert dict(out.dtypes)["market_key"] == "bigint"
    assert out.first()["updated_at"] is not None


def test__silver_dims_refuses_a_value_that_is_not_the_right_type(spark: SparkSession) -> None:
    assert transform__market(spark, [("abc", "Gujarat", "Ahmedabad", "M1")]) is None


def test__silver_dims_refuses_an_empty_key_or_name(spark: SparkSession) -> None:
    assert transform__market(spark, [(None, "Gujarat", "Ahmedabad", "M1")]) is None
    assert transform__market(spark, [("1", "Gujarat", "Ahmedabad", None)]) is None


def test__silver_dims_refuses_a_duplicate_key(spark: SparkSession) -> None:
    assert transform__market(spark, [("1", "Gujarat", "Ahmedabad", "M1"), ("1", "Gujarat", "Surat", "M2")]) is None


def test__silver_dims_refuses_a_duplicate_natural_key(spark: SparkSession) -> None:
    assert transform__market(spark, [("1", "Gujarat", "Ahmedabad", "M1"), ("2", "Gujarat", "Ahmedabad", "M1")]) is None


def test__silver_dims_date_refuses_a_missing_day(spark: SparkSession) -> None:
    from lib_mandi.config import RULES_DIM_DATE, SCHEMA_DIM_DATE

    def run(days: list[str]) -> DataFrame | None:
        rows = [(d.replace("-", ""), d, "1", "Mon", d, "1", "Jan", "1", "2026", "false", "Rabi") for d in days]
        df = spark.createDataFrame(rows, ", ".join(f"{f.name} string" for f in SCHEMA_DIM_DATE.fields))
        return t.transform__silver_dims(
            df,
            SCHEMA_DIM_DATE,
            RULES_DIM_DATE["not_null_columns"],
            RULES_DIM_DATE["unique_columns"],
            RULES_DIM_DATE["date_column"],
        )

    assert run(["2026-01-01", "2026-01-02", "2026-01-03"]) is not None
    assert run(["2026-01-01", "2026-01-03"]) is None


# --- dq checks ---

def test__dq_check_appends_rule_id_and_reason_only_to_failing_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("", ["earlier"], ["Earlier reason"]), ("M1", [], [])],
        "t_market string, rule_ids array<string>, rule_reasons array<string>",
    )
    rows = {r["t_market"]: r for r in dq_check__missing_market(df).collect()}
    assert rows[""]["rule_ids"] == ["earlier", "missing_market"]
    assert rows[""]["rule_reasons"] == ["Earlier reason", "Market is missing"]
    assert rows["M1"]["rule_ids"] == [] and rows["M1"]["rule_reasons"] == []
