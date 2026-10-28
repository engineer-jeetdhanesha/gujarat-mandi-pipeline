from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from lib_mandi.config import OPTIONS_READ_SEED, OPTIONS_READ_SOURCE, SCHEMA_RAW_DIM_MARKET, SCHEMA_SOURCE
from lib_mandi.extract import (
    extract__bronze,
    extract__bronze_dims,
    extract__gold_weekly_mart,
    extract__silver_dims,
    extract__silver_facts,
    extract__silver_transactions,
)

SOURCE_HEADER = "State,District,Market,Commodity,Commodity_Code,Variety,Grade,Arrival_Date,Min_Price,Max_Price,Modal_Price\n"
SOURCE_ROW = "Gujarat,Ahmedabad,M1,Wheat,100,Other,FAQ,05/01/2026,10,30,20\n"
DIM_HEADER = "market_key,state,district_name,market_name\n"


# --- bronze: the daily source file ---

def test__extract_bronze_reads_every_row_as_strings(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "mandi_2026-01-05.csv"
    path.write_text(SOURCE_HEADER + SOURCE_ROW + SOURCE_ROW.replace(",10,", ",11,"))

    df = extract__bronze(spark, str(path), SCHEMA_SOURCE, OPTIONS_READ_SOURCE)

    # Bronze keeps the values exactly as received, so every column is a string.
    assert df is not None
    assert df.columns == SCHEMA_SOURCE.names
    assert {data_type for _, data_type in df.dtypes} == {"string"}
    assert sorted(row["min_price"] for row in df.collect()) == ["10", "11"]


def test__extract_bronze_returns_none_for_a_file_without_data_rows(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text(SOURCE_HEADER)

    assert extract__bronze(spark, str(path), SCHEMA_SOURCE, OPTIONS_READ_SOURCE) is None


def test__extract_bronze_returns_none_for_a_row_that_does_not_fit(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text(SOURCE_HEADER + "Gujarat,Ahmedabad,M1\n")  # 3 values instead of 11

    assert extract__bronze(spark, str(path), SCHEMA_SOURCE, OPTIONS_READ_SOURCE) is None


def test__extract_bronze_returns_none_for_a_missing_file(spark: SparkSession, tmp_path: Path) -> None:
    assert extract__bronze(spark, str(tmp_path / "missing.csv"), SCHEMA_SOURCE, OPTIONS_READ_SOURCE) is None


# --- bronze: the seed files of the dimensions ---

def test__extract_bronze_dims_reads_the_file_as_received(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "dim_market.csv"
    path.write_text(DIM_HEADER + "1,Gujarat,Ahmedabad,M1\n1,Gujarat,Ahmedabad,M1\n")

    df = extract__bronze_dims(spark, str(path), SCHEMA_RAW_DIM_MARKET, OPTIONS_READ_SEED)

    assert df is not None
    assert df.columns == SCHEMA_RAW_DIM_MARKET.names
    # The duplicate is kept in bronze; the silver checks refuse it later.
    assert df.count() == 2


def test__extract_bronze_dims_returns_none_for_a_file_without_data_rows(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text(DIM_HEADER)

    assert extract__bronze_dims(spark, str(path), SCHEMA_RAW_DIM_MARKET, OPTIONS_READ_SEED) is None


def test__extract_bronze_dims_returns_none_for_a_wrong_header(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "wrong_header.csv"
    path.write_text("key,state,district_name,market_name\n1,Gujarat,Ahmedabad,M1\n")  # "key" instead of "market_key"

    assert extract__bronze_dims(spark, str(path), SCHEMA_RAW_DIM_MARKET, OPTIONS_READ_SEED) is None


def test__extract_bronze_dims_returns_none_for_a_file_that_is_not_a_csv(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "dim_market.txt"
    path.write_text("")

    assert extract__bronze_dims(spark, str(path), SCHEMA_RAW_DIM_MARKET, OPTIONS_READ_SEED) is None


# --- silver dimensions: read the bronze seed table ---

def test__extract_silver_dims_returns_only_the_rows_of_the_run(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("1", "run1"), ("2", "run1"), ("9", "run0")], "market_key string, _run_id string"
    ).write.format("delta").mode("overwrite").saveAsTable("test_extract_silver_dims_a")

    df = extract__silver_dims(spark, "run1", "test_extract_silver_dims_a")

    # Row "9" belongs to an earlier run and must not be loaded again.
    assert df is not None
    assert sorted(row["market_key"] for row in df.collect()) == ["1", "2"]


def test__extract_silver_dims_returns_none_for_a_run_without_rows(spark: SparkSession) -> None:
    spark.createDataFrame(
        [("1", "run1")], "market_key string, _run_id string"
    ).write.format("delta").mode("overwrite").saveAsTable("test_extract_silver_dims_b")

    assert extract__silver_dims(spark, "run2", "test_extract_silver_dims_b") is None


def test__extract_silver_dims_returns_none_for_a_missing_table(spark: SparkSession) -> None:
    assert extract__silver_dims(spark, "run1", "test_no_such_table") is None


# --- silver transactions: bronze rows of the run plus the three dimensions ---

def test__extract_silver_transactions_returns_the_rows_of_the_run_and_the_dimensions(spark: SparkSession) -> None:
    # Two bronze rows from run1, one from run2.
    spark.createDataFrame(
        [("M1", "run1"), ("M2", "run1"), ("M3", "run2")], "market string, _run_id string"
    ).write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_a_bronze")
    spark.createDataFrame([(1, "M1")], "market_key int, market_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_a_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_a_commodity")
    spark.createDataFrame([(20260105,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_a_date")

    result = extract__silver_transactions(
        spark, "run1", "test_extract_tx_a_bronze", "test_extract_tx_a_market", "test_extract_tx_a_commodity", "test_extract_tx_a_date"
    )

    assert result is not None
    assert set(result) == {"bronze", "dim_market", "dim_commodity", "dim_date"}
    assert result["bronze"].count() == 2  # run2 is left out
    assert result["dim_market"].count() == 1


def test__extract_silver_transactions_returns_none_for_a_run_without_rows(spark: SparkSession) -> None:
    spark.createDataFrame([("M1", "run1")], "market string, _run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_b_bronze")
    spark.createDataFrame([(1, "M1")], "market_key int, market_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_b_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_b_commodity")
    spark.createDataFrame([(20260105,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_b_date")

    result = extract__silver_transactions(
        spark, "run2", "test_extract_tx_b_bronze", "test_extract_tx_b_market", "test_extract_tx_b_commodity", "test_extract_tx_b_date"
    )

    assert result is None


def test__extract_silver_transactions_returns_none_when_a_dimension_is_empty(spark: SparkSession) -> None:
    spark.createDataFrame([("M1", "run1")], "market string, _run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_c_bronze")
    spark.createDataFrame([(1, "M1")], "market_key int, market_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_c_market")
    # No commodities loaded yet: the silver load cannot look anything up.
    spark.createDataFrame([], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_c_commodity")
    spark.createDataFrame([(20260105,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_c_date")

    result = extract__silver_transactions(
        spark, "run1", "test_extract_tx_c_bronze", "test_extract_tx_c_market", "test_extract_tx_c_commodity", "test_extract_tx_c_date"
    )

    assert result is None


def test__extract_silver_transactions_returns_none_for_a_missing_table(spark: SparkSession) -> None:
    spark.createDataFrame([(1, "M1")], "market_key int, market_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_d_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_d_commodity")
    spark.createDataFrame([(20260105,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_tx_d_date")

    result = extract__silver_transactions(
        spark, "run1", "test_no_such_table", "test_extract_tx_d_market", "test_extract_tx_d_commodity", "test_extract_tx_d_date"
    )

    assert result is None


# --- silver facts: clean rows of the run plus the three dimensions ---

def test__extract_silver_facts_returns_the_rows_of_the_run_and_the_dimensions(spark: SparkSession) -> None:
    # One clean row from run1, one from run2.
    spark.createDataFrame([("M1", "run1"), ("M2", "run2")], "market string, run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_a_clean")
    spark.createDataFrame([(1, "M1")], "market_key int, market_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_a_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_a_commodity")
    spark.createDataFrame([(20260105,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_a_date")

    result = extract__silver_facts(
        spark, "run1", "test_extract_fact_a_clean", "test_extract_fact_a_market", "test_extract_fact_a_commodity", "test_extract_fact_a_date"
    )

    assert result is not None
    assert set(result) == {"clean", "dim_market", "dim_commodity", "dim_date"}
    assert result["clean"].count() == 1  # run2 is left out


def test__extract_silver_facts_returns_none_for_a_run_without_rows(spark: SparkSession) -> None:
    spark.createDataFrame([("M1", "run1")], "market string, run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_b_clean")
    spark.createDataFrame([(1, "M1")], "market_key int, market_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_b_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_b_commodity")
    spark.createDataFrame([(20260105,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_b_date")

    result = extract__silver_facts(
        spark, "run2", "test_extract_fact_b_clean", "test_extract_fact_b_market", "test_extract_fact_b_commodity", "test_extract_fact_b_date"
    )

    assert result is None


def test__extract_silver_facts_returns_none_for_a_missing_table(spark: SparkSession) -> None:
    spark.createDataFrame([("M1", "run1")], "market string, run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_c_clean")
    spark.createDataFrame([(1, "M1")], "market_key int, market_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_c_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_fact_c_commodity")

    result = extract__silver_facts(
        spark, "run1", "test_extract_fact_c_clean", "test_extract_fact_c_market", "test_extract_fact_c_commodity", "test_no_such_table"
    )

    assert result is None


# --- gold weekly mart: clean rows of the run, the fact table and the three dimensions ---

def test__extract_gold_returns_the_rows_of_the_run_the_fact_and_the_dimensions(spark: SparkSession) -> None:
    # One clean row from run1, one from run2; the fact table has one row.
    spark.createDataFrame([(date(2026, 12, 14), "run1"), (date(2026, 12, 21), "run2")], "arrival_date date, run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_a_clean")
    spark.createDataFrame([(1, 20261214)], "market_key int, date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_a_fact")
    spark.createDataFrame([(1, "D1")], "market_key int, district_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_a_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_a_commodity")
    spark.createDataFrame([(20261214,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_a_date")

    result = extract__gold_weekly_mart(
        spark, "run1", "test_extract_gold_a_clean", "test_extract_gold_a_fact",
        "test_extract_gold_a_market", "test_extract_gold_a_commodity", "test_extract_gold_a_date",
    )

    assert result is not None
    assert set(result) == {"clean", "fact", "dim_market", "dim_commodity", "dim_date"}
    assert result["clean"].count() == 1  # run2 is left out
    assert result["fact"].count() == 1


def test__extract_gold_returns_none_for_a_run_without_rows(spark: SparkSession) -> None:
    spark.createDataFrame([(date(2026, 12, 14), "run1")], "arrival_date date, run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_b_clean")
    spark.createDataFrame([(1, 20261214)], "market_key int, date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_b_fact")
    spark.createDataFrame([(1, "D1")], "market_key int, district_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_b_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_b_commodity")
    spark.createDataFrame([(20261214,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_b_date")

    result = extract__gold_weekly_mart(
        spark, "run2", "test_extract_gold_b_clean", "test_extract_gold_b_fact",
        "test_extract_gold_b_market", "test_extract_gold_b_commodity", "test_extract_gold_b_date",
    )

    assert result is None


def test__extract_gold_returns_none_when_the_fact_table_is_empty(spark: SparkSession) -> None:
    spark.createDataFrame([(date(2026, 12, 14), "run1")], "arrival_date date, run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_c_clean")
    # The facts have not been loaded yet, so there is nothing to aggregate.
    spark.createDataFrame([], "market_key int, date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_c_fact")
    spark.createDataFrame([(1, "D1")], "market_key int, district_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_c_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_c_commodity")
    spark.createDataFrame([(20261214,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_c_date")

    result = extract__gold_weekly_mart(
        spark, "run1", "test_extract_gold_c_clean", "test_extract_gold_c_fact",
        "test_extract_gold_c_market", "test_extract_gold_c_commodity", "test_extract_gold_c_date",
    )

    assert result is None


def test__extract_gold_returns_none_for_a_missing_table(spark: SparkSession) -> None:
    spark.createDataFrame([(date(2026, 12, 14), "run1")], "arrival_date date, run_id string").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_d_clean")
    spark.createDataFrame([(1, "D1")], "market_key int, district_name string").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_d_market")
    spark.createDataFrame([(10, 100)], "commodity_key int, commodity_code int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_d_commodity")
    spark.createDataFrame([(20261214,)], "date_key int").write.format("delta").mode("overwrite").saveAsTable("test_extract_gold_d_date")

    result = extract__gold_weekly_mart(
        spark, "run1", "test_extract_gold_d_clean", "test_no_such_table",
        "test_extract_gold_d_market", "test_extract_gold_d_commodity", "test_extract_gold_d_date",
    )

    assert result is None
