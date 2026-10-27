from pathlib import Path

from pyspark.sql import SparkSession

from lib_mandi.config import (
    OPTIONS_READ_SEED,
    OPTIONS_READ_SOURCE,
    RULES_DIM_MARKET,
    SCHEMA_DIM_MARKET,
    SCHEMA_RAW_DIM_MARKET,
    SCHEMA_SOURCE,
)
from lib_mandi.extract import extract__bronze, extract__bronze_dims, extract__silver_dims
from lib_mandi.load import load__bronze, load__silver_dims
from lib_mandi.transformations import transform__bronze, transform__silver_dims

SOURCE_HEADER = "State,District,Market,Commodity,Commodity_Code,Variety,Grade,Arrival_Date,Min_Price,Max_Price,Modal_Price\n"
SOURCE_ROW = "Gujarat,Ahmedabad,M1,Wheat,100,Other,FAQ,05/01/2026,10,30,20\n"
DIM_HEADER = "market_key,state,district_name,market_name\n"


# --- bronze ---

def test__extract_transform_load_bronze(spark: SparkSession, tmp_path: Path) -> None:
    table = "test_raw_mandi_transactions"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    path = tmp_path / "mandi_2026-01-05.csv"
    path.write_text(SOURCE_HEADER + SOURCE_ROW + SOURCE_ROW.replace(",10,", ",11,"))
    try:
        df = extract__bronze(spark, str(path), SCHEMA_SOURCE, OPTIONS_READ_SOURCE)
        assert df is not None
        df = transform__bronze(df, "run1", path.name)
        assert df is not None
        assert load__bronze(df, table) is True

        rows = spark.table(table).orderBy("_row_no").collect()
        assert [r["_row_no"] for r in rows] == [1, 2]
        assert [r["min_price"] for r in rows] == ["10", "11"]
        assert {(r["_run_id"], r["_file_name"], r["_source_mode"]) for r in rows} == {("run1", path.name, "FILE")}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test__extract_bronze_returns_none_for_file_without_data_rows(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text(SOURCE_HEADER)
    assert extract__bronze(spark, str(path), SCHEMA_SOURCE, OPTIONS_READ_SOURCE) is None


def test__extract_bronze_returns_none_for_row_that_does_not_fit(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text(SOURCE_HEADER + "Gujarat,Ahmedabad,M1\n")
    assert extract__bronze(spark, str(path), SCHEMA_SOURCE, OPTIONS_READ_SOURCE) is None


def test__extract_bronze_returns_none_for_missing_file(spark: SparkSession, tmp_path: Path) -> None:
    assert extract__bronze(spark, str(tmp_path / "missing.csv"), SCHEMA_SOURCE, OPTIONS_READ_SOURCE) is None


# --- bronze seed tables ---

def test__extract_transform_load_bronze_dims_appends_the_file_as_received(spark: SparkSession, tmp_path: Path) -> None:
    table = "test_raw_dim_market"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    path = tmp_path / "dim_market.csv"
    path.write_text(DIM_HEADER + "1,Gujarat,Ahmedabad,M1\n1,Gujarat,Ahmedabad,M1\n")
    try:
        df = extract__bronze_dims(spark, str(path), SCHEMA_RAW_DIM_MARKET, OPTIONS_READ_SEED)
        assert df is not None
        df = transform__bronze(df, "run1", path.name)
        assert df is not None
        assert load__bronze(df, table) is True

        rows = spark.table(table).collect()
        assert len(rows) == 2  # duplicates are kept in bronze; silver refuses them
        assert {(r["market_key"], r["_run_id"], r["_file_name"]) for r in rows} == {("1", "run1", path.name)}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test__extract_bronze_dims_returns_none_for_bad_files(spark: SparkSession, tmp_path: Path) -> None:
    empty = tmp_path / "empty.csv"
    empty.write_text(DIM_HEADER)
    wrong_header = tmp_path / "wrong_header.csv"
    wrong_header.write_text("key,state,district_name,market_name\n1,Gujarat,Ahmedabad,M1\n")
    not_csv = tmp_path / "dim_market.txt"
    not_csv.write_text("")
    for path in (empty, wrong_header, not_csv):
        assert extract__bronze_dims(spark, str(path), SCHEMA_RAW_DIM_MARKET, OPTIONS_READ_SEED) is None


# --- silver dimensions ---

def create_dim_market(spark: SparkSession, table: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"CREATE TABLE {table} (market_key BIGINT, state STRING, district_name STRING, market_name STRING, "
        "updated_at TIMESTAMP) USING parquet"
    )


def create_raw_dim_market(spark: SparkSession, table: str, rows: list[tuple[str, str, str, str, str]]) -> None:
    """Bronze seed table with (market_key, state, district_name, market_name, _run_id) rows."""
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.createDataFrame(
        rows, "market_key string, state string, district_name string, market_name string, _run_id string"
    ).write.saveAsTable(table)


def run__silver_dims(spark: SparkSession, run_id: str, raw_table: str, silver_table: str) -> bool:
    """extract -> transform -> load for dim_market; False if a step returned None."""
    df = extract__silver_dims(spark, run_id, raw_table)
    if df is not None:
        df = transform__silver_dims(
            df,
            SCHEMA_DIM_MARKET,
            RULES_DIM_MARKET["not_null_columns"],
            RULES_DIM_MARKET["unique_columns"],
            RULES_DIM_MARKET["date_column"],
        )
    return df is not None and load__silver_dims(df, silver_table)


def test__silver_dims_loads_the_rows_of_the_run_and_replaces_the_table(spark: SparkSession) -> None:
    raw, silver = "test_raw_dim_market_a", "test_dim_market_a"
    create_raw_dim_market(spark, raw, [("1", "Gujarat", "Ahmedabad", "M1", "run1"), ("2", "Gujarat", "Surat", "M2", "run1"), ("9", "x", "x", "other run", "run0")])
    create_dim_market(spark, silver)
    spark.sql(f"INSERT INTO {silver} VALUES (99, 'x', 'x', 'old', current_timestamp())")
    try:
        assert run__silver_dims(spark, "run1", raw, silver) is True

        rows = spark.table(silver).orderBy("market_key").collect()
        assert [(r["market_key"], r["market_name"]) for r in rows] == [(1, "M1"), (2, "M2")]
        assert all(r["updated_at"] is not None for r in rows)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {raw}")
        spark.sql(f"DROP TABLE IF EXISTS {silver}")


def test__silver_dims_leaves_the_table_unchanged_when_a_check_fails(spark: SparkSession) -> None:
    raw, silver = "test_raw_dim_market_b", "test_dim_market_b"
    create_raw_dim_market(spark, raw, [("1", "Gujarat", "Ahmedabad", "M1", "run1"), ("1", "Gujarat", "Surat", "M2", "run1")])
    create_dim_market(spark, silver)
    spark.sql(f"INSERT INTO {silver} VALUES (99, 'x', 'x', 'old', current_timestamp())")
    try:
        assert run__silver_dims(spark, "run1", raw, silver) is False

        assert [r["market_name"] for r in spark.table(silver).collect()] == ["old"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {raw}")
        spark.sql(f"DROP TABLE IF EXISTS {silver}")


def test__extract_silver_dims_returns_none_for_a_run_without_rows(spark: SparkSession) -> None:
    raw = "test_raw_dim_market_c"
    create_raw_dim_market(spark, raw, [("1", "Gujarat", "Ahmedabad", "M1", "run1")])
    try:
        assert extract__silver_dims(spark, "run2", raw) is None
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {raw}")
