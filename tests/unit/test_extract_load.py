from pathlib import Path

from pyspark.sql import SparkSession

from lib_mandi.config import OPTIONS_READ_SEED, OPTIONS_READ_SOURCE, SCHEMA_DIM_MARKET, SCHEMA_SOURCE
from lib_mandi.extract import extract__bronze, extract__silver_dims
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


# --- silver dimensions ---

def create_dim_market(spark: SparkSession, table: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"CREATE TABLE {table} (market_key BIGINT, state STRING, district_name STRING, market_name STRING, "
        "updated_at TIMESTAMP) USING parquet"
    )


def test__extract_transform_load_silver_dims_replaces_table_rows(spark: SparkSession, tmp_path: Path) -> None:
    table = "test_dim_market"
    create_dim_market(spark, table)
    spark.sql(f"INSERT INTO {table} VALUES (99, 'x', 'x', 'old', current_timestamp())")
    path = tmp_path / "dim_market.csv"
    path.write_text(DIM_HEADER + "1,Gujarat,Ahmedabad,M1\n2,Gujarat,Surat,M2\n")
    try:
        df = extract__silver_dims(spark, str(path), SCHEMA_DIM_MARKET, OPTIONS_READ_SEED)
        assert df is not None
        df = transform__silver_dims(df)
        assert df is not None
        assert load__silver_dims(df, table) is True

        rows = spark.table(table).orderBy("market_key").collect()
        assert [(r["market_key"], r["market_name"]) for r in rows] == [(1, "M1"), (2, "M2")]
        assert all(r["updated_at"] is not None for r in rows)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test__extract_silver_dims_returns_none_for_bad_files(spark: SparkSession, tmp_path: Path) -> None:
    empty = tmp_path / "empty.csv"
    empty.write_text(DIM_HEADER)
    wrong_header = tmp_path / "wrong_header.csv"
    wrong_header.write_text("key,state,district_name,market_name\n1,Gujarat,Ahmedabad,M1\n")
    not_csv = tmp_path / "dim_market.txt"
    not_csv.write_text("")
    for path in (empty, wrong_header, not_csv):
        assert extract__silver_dims(spark, str(path), SCHEMA_DIM_MARKET, OPTIONS_READ_SEED) is None
