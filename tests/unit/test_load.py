from datetime import date
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
from lib_mandi.load import (
    load__bronze,
    load__gold_weekly_mart,
    load__silver_dims,
    load__silver_facts,
    load__silver_transactions,
)
from lib_mandi.transformations import transform__bronze, transform__silver_dims

SOURCE_HEADER = "State,District,Market,Commodity,Commodity_Code,Variety,Grade,Arrival_Date,Min_Price,Max_Price,Modal_Price\n"
SOURCE_ROW = "Gujarat,Ahmedabad,M1,Wheat,100,Other,FAQ,05/01/2026,10,30,20\n"
DIM_HEADER = "market_key,state,district_name,market_name\n"

# Silver dimension tables: the typed columns plus updated_at, as in silver.dim_market.
DIM_MARKET_COLUMNS = "market_key BIGINT, state STRING, district_name STRING, market_name STRING, updated_at TIMESTAMP"
# Bronze seed table: every value as a string, plus the run that loaded it.
RAW_DIM_MARKET_COLUMNS = "market_key string, state string, district_name string, market_name string, _run_id string"

# Reduced versions of the silver and gold tables: the key columns the MERGE matches on,
# plus one value column to see whether a row was updated.
CLEAN_COLUMNS = "market string, commodity_code int, variety string, grade string, arrival_date date, modal_price int, run_id string"
REJECTED_COLUMNS = "run_id string, file_name string, row_no bigint, reject_rule string"
FACT_COLUMNS = "market_key bigint, commodity_key bigint, date_key int, variety string, grade string, modal_price int"
MART_COLUMNS = "district_name string, commodity_code int, week_start date, avg_modal_price int"

DAY = date(2026, 1, 5)
WEEK_1, WEEK_2 = date(2026, 12, 7), date(2026, 12, 14)


# --- bronze ---

def test__load_bronze_appends_the_rows_with_their_metadata(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "mandi_2026-01-05.csv"
    path.write_text(SOURCE_HEADER + SOURCE_ROW + SOURCE_ROW.replace(",10,", ",11,"))
    df = extract__bronze(spark, str(path), SCHEMA_SOURCE, OPTIONS_READ_SOURCE)
    df = transform__bronze(df, "run1", path.name)
    # An empty table with the same columns as the file plus the metadata columns.
    df.limit(0).write.format("delta").mode("overwrite").saveAsTable("test_load_bronze_a")

    # Loading the same rows twice appends twice: bronze never replaces or removes rows.
    assert load__bronze(df, "test_load_bronze_a") is True
    assert load__bronze(df, "test_load_bronze_a") is True

    rows = spark.table("test_load_bronze_a").orderBy("_row_no").collect()
    assert [row["_row_no"] for row in rows] == [1, 1, 2, 2]
    assert [row["min_price"] for row in rows] == ["10", "10", "11", "11"]
    assert {(row["_run_id"], row["_file_name"], row["_source_mode"]) for row in rows} == {("run1", path.name, "FILE")}


def test__load_bronze_returns_false_and_adds_nothing_when_the_columns_do_not_fit(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "mandi_2026-01-05.csv"
    path.write_text(SOURCE_HEADER + SOURCE_ROW)
    df = extract__bronze(spark, str(path), SCHEMA_SOURCE, OPTIONS_READ_SOURCE)
    df = transform__bronze(df, "run1", path.name)
    # A table with only two of the columns.
    spark.createDataFrame([("9", "run0")], "min_price string, _run_id string").write.format("delta").mode("overwrite").saveAsTable("test_load_bronze_b")

    assert load__bronze(df, "test_load_bronze_b") is False

    assert spark.table("test_load_bronze_b").count() == 1


def test__load_bronze_keeps_duplicate_rows_of_a_seed_file(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "dim_market.csv"
    path.write_text(DIM_HEADER + "1,Gujarat,Ahmedabad,M1\n1,Gujarat,Ahmedabad,M1\n")
    df = extract__bronze_dims(spark, str(path), SCHEMA_RAW_DIM_MARKET, OPTIONS_READ_SEED)
    df = transform__bronze(df, "run1", path.name)
    df.limit(0).write.format("delta").mode("overwrite").saveAsTable("test_load_bronze_c")

    assert load__bronze(df, "test_load_bronze_c") is True

    rows = spark.table("test_load_bronze_c").collect()
    # Both copies of the duplicate are in bronze; the silver checks refuse them later.
    assert len(rows) == 2
    assert {(row["market_key"], row["_run_id"], row["_file_name"]) for row in rows} == {("1", "run1", "dim_market.csv")}


# --- silver dimensions ---

def test__silver_dims_loads_the_rows_of_the_run_and_replaces_the_table(spark: SparkSession) -> None:
    # Bronze holds the seed rows of run1 and an older run0; silver still holds a row from before.
    spark.sql(f"CREATE TABLE test_load_dims_a_raw ({RAW_DIM_MARKET_COLUMNS}) USING delta")
    spark.sql("INSERT INTO test_load_dims_a_raw VALUES ('1', 'Gujarat', 'Ahmedabad', 'M1', 'run1'), ('2', 'Gujarat', 'Surat', 'M2', 'run1'), ('9', 'x', 'x', 'other run', 'run0')")
    spark.sql(f"CREATE TABLE test_load_dims_a_silver ({DIM_MARKET_COLUMNS}) USING delta")
    spark.sql("INSERT INTO test_load_dims_a_silver VALUES (99, 'x', 'x', 'old', current_timestamp())")

    # extract -> transform -> load, the same three calls as the load_silver_dims notebook.
    df = extract__silver_dims(spark, "run1", "test_load_dims_a_raw")
    df = transform__silver_dims(
        df,
        SCHEMA_DIM_MARKET,
        RULES_DIM_MARKET["not_null_columns"],
        RULES_DIM_MARKET["unique_columns"],
        RULES_DIM_MARKET["date_column"],
    )
    assert load__silver_dims(df, "test_load_dims_a_silver") is True

    # The old row is gone, the run0 row was not loaded, and every row has its updated_at.
    rows = spark.table("test_load_dims_a_silver").orderBy("market_key").collect()
    assert [(row["market_key"], row["market_name"]) for row in rows] == [(1, "M1"), (2, "M2")]
    assert all(row["updated_at"] is not None for row in rows)


def test__silver_dims_leaves_the_table_unchanged_when_a_check_fails(spark: SparkSession) -> None:
    # Both rows have market_key 1, so the unique check refuses them.
    spark.sql(f"CREATE TABLE test_load_dims_b_raw ({RAW_DIM_MARKET_COLUMNS}) USING delta")
    spark.sql("INSERT INTO test_load_dims_b_raw VALUES ('1', 'Gujarat', 'Ahmedabad', 'M1', 'run1'), ('1', 'Gujarat', 'Surat', 'M2', 'run1')")
    spark.sql(f"CREATE TABLE test_load_dims_b_silver ({DIM_MARKET_COLUMNS}) USING delta")
    spark.sql("INSERT INTO test_load_dims_b_silver VALUES (99, 'x', 'x', 'old', current_timestamp())")

    df = extract__silver_dims(spark, "run1", "test_load_dims_b_raw")
    df = transform__silver_dims(
        df,
        SCHEMA_DIM_MARKET,
        RULES_DIM_MARKET["not_null_columns"],
        RULES_DIM_MARKET["unique_columns"],
        RULES_DIM_MARKET["date_column"],
    )

    # The transform returns None, so there is nothing to load and silver keeps its old row.
    assert df is None
    assert [row["market_name"] for row in spark.table("test_load_dims_b_silver").collect()] == ["old"]


def test__load_silver_dims_returns_false_for_a_missing_table(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "Gujarat", "Ahmedabad", "M1")], "market_key bigint, state string, district_name string, market_name string"
    )

    assert load__silver_dims(df, "test_no_such_table") is False


# --- silver transactions ---

def test__load_silver_transactions_inserts_new_rows_and_updates_a_row_with_the_same_key(spark: SparkSession) -> None:
    spark.sql(f"CREATE TABLE test_load_tx_a_clean ({CLEAN_COLUMNS}) USING delta")
    spark.sql(f"CREATE TABLE test_load_tx_a_rejected ({REJECTED_COLUMNS}) USING delta")
    no_rejected = spark.createDataFrame([], REJECTED_COLUMNS)
    first_run = spark.createDataFrame(
        [("M1", 100, "Other", "FAQ", DAY, 20, "run1"), ("M2", 100, "Other", "FAQ", DAY, 25, "run1")], CLEAN_COLUMNS
    )
    # The second run delivers a new price for M1 only.
    second_run = spark.createDataFrame([("M1", 100, "Other", "FAQ", DAY, 22, "run2")], CLEAN_COLUMNS)

    assert load__silver_transactions(spark, first_run, no_rejected, "test_load_tx_a_clean", "test_load_tx_a_rejected") is True
    assert load__silver_transactions(spark, second_run, no_rejected, "test_load_tx_a_clean", "test_load_tx_a_rejected") is True

    # M1 was updated in place (still one row), M2 is untouched.
    rows = {row["market"]: row for row in spark.table("test_load_tx_a_clean").collect()}
    assert set(rows) == {"M1", "M2"}
    assert (rows["M1"]["modal_price"], rows["M1"]["run_id"]) == (22, "run2")
    assert (rows["M2"]["modal_price"], rows["M2"]["run_id"]) == (25, "run1")


def test__load_silver_transactions_matches_rows_with_an_empty_variety_and_grade(spark: SparkSession) -> None:
    spark.sql(f"CREATE TABLE test_load_tx_b_clean ({CLEAN_COLUMNS}) USING delta")
    spark.sql(f"CREATE TABLE test_load_tx_b_rejected ({REJECTED_COLUMNS}) USING delta")
    no_rejected = spark.createDataFrame([], REJECTED_COLUMNS)
    first_run = spark.createDataFrame([("M1", 100, None, None, DAY, 20, "run1")], CLEAN_COLUMNS)
    second_run = spark.createDataFrame([("M1", 100, None, None, DAY, 21, "run2")], CLEAN_COLUMNS)

    assert load__silver_transactions(spark, first_run, no_rejected, "test_load_tx_b_clean", "test_load_tx_b_rejected") is True
    assert load__silver_transactions(spark, second_run, no_rejected, "test_load_tx_b_clean", "test_load_tx_b_rejected") is True

    # NULL = NULL is not true in SQL, so the MERGE must compare null-safe (<=>) or the row would be added twice.
    assert [row["modal_price"] for row in spark.table("test_load_tx_b_clean").collect()] == [21]


def test__load_silver_transactions_inserts_a_rejected_row_only_once(spark: SparkSession) -> None:
    spark.sql(f"CREATE TABLE test_load_tx_c_clean ({CLEAN_COLUMNS}) USING delta")
    spark.sql(f"CREATE TABLE test_load_tx_c_rejected ({REJECTED_COLUMNS}) USING delta")
    no_clean = spark.createDataFrame([], CLEAN_COLUMNS)
    first_run = spark.createDataFrame([("run1", "f.csv", 1, "missing_market")], REJECTED_COLUMNS)
    # A rerun of the same run: row 1 again (with a different rule text) and a new row 2.
    rerun = spark.createDataFrame(
        [("run1", "f.csv", 1, "changed"), ("run1", "f.csv", 2, "invalid_price")], REJECTED_COLUMNS
    )

    assert load__silver_transactions(spark, no_clean, first_run, "test_load_tx_c_clean", "test_load_tx_c_rejected") is True
    assert load__silver_transactions(spark, no_clean, rerun, "test_load_tx_c_clean", "test_load_tx_c_rejected") is True

    # Rejected rows are insert-only: row 1 keeps its first value and row 2 is added.
    rows = {row["row_no"]: row["reject_rule"] for row in spark.table("test_load_tx_c_rejected").collect()}
    assert rows == {1: "missing_market", 2: "invalid_price"}


def test__load_silver_transactions_returns_false_for_a_missing_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE TABLE test_load_tx_d_rejected ({REJECTED_COLUMNS}) USING delta")
    clean = spark.createDataFrame([("M1", 100, "Other", "FAQ", DAY, 20, "run1")], CLEAN_COLUMNS)
    no_rejected = spark.createDataFrame([], REJECTED_COLUMNS)

    assert load__silver_transactions(spark, clean, no_rejected, "test_no_such_table", "test_load_tx_d_rejected") is False


# --- silver facts ---

def test__load_silver_facts_inserts_new_rows_and_updates_a_row_with_the_same_key(spark: SparkSession) -> None:
    spark.sql(f"CREATE TABLE test_load_fact_a ({FACT_COLUMNS}) USING delta")
    first_run = spark.createDataFrame(
        [(1, 10, 20260105, "Other", "FAQ", 20), (2, 10, 20260105, "Other", "FAQ", 25)], FACT_COLUMNS
    )
    # The second run delivers a new price for market 1 only.
    second_run = spark.createDataFrame([(1, 10, 20260105, "Other", "FAQ", 22)], FACT_COLUMNS)

    assert load__silver_facts(spark, first_run, "test_load_fact_a") is True
    assert load__silver_facts(spark, second_run, "test_load_fact_a") is True

    assert {row["market_key"]: row["modal_price"] for row in spark.table("test_load_fact_a").collect()} == {1: 22, 2: 25}


def test__load_silver_facts_matches_rows_with_an_empty_variety_and_grade(spark: SparkSession) -> None:
    spark.sql(f"CREATE TABLE test_load_fact_b ({FACT_COLUMNS}) USING delta")
    first_run = spark.createDataFrame([(1, 10, 20260105, None, None, 20)], FACT_COLUMNS)
    second_run = spark.createDataFrame([(1, 10, 20260105, None, None, 21)], FACT_COLUMNS)

    assert load__silver_facts(spark, first_run, "test_load_fact_b") is True
    assert load__silver_facts(spark, second_run, "test_load_fact_b") is True

    # One row, not two: the null-safe match works for the fact table as well.
    assert [row["modal_price"] for row in spark.table("test_load_fact_b").collect()] == [21]


def test__load_silver_facts_returns_false_for_a_missing_table(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 10, 20260105, "Other", "FAQ", 20)], FACT_COLUMNS)

    assert load__silver_facts(spark, df, "test_no_such_table") is False


# --- gold weekly mart ---

def test__load_gold_updates_an_existing_week_and_inserts_new_ones(spark: SparkSession) -> None:
    spark.sql(f"CREATE TABLE test_load_gold_a ({MART_COLUMNS}) USING delta")
    spark.sql("INSERT INTO test_load_gold_a VALUES ('D1', 100, DATE'2026-12-07', 30)")
    # D1 week 1 already exists (new average 31); D1 week 2 and D2 week 1 are new.
    df = spark.createDataFrame(
        [("D1", 100, WEEK_1, 31), ("D1", 100, WEEK_2, 45), ("D2", 100, WEEK_1, 50)], MART_COLUMNS
    )

    assert load__gold_weekly_mart(spark, df, "test_load_gold_a") is True

    rows = {(row["district_name"], row["week_start"]): row["avg_modal_price"] for row in spark.table("test_load_gold_a").collect()}
    assert rows == {("D1", WEEK_1): 31, ("D1", WEEK_2): 45, ("D2", WEEK_1): 50}


def test__load_gold_returns_false_for_a_missing_table(spark: SparkSession) -> None:
    df = spark.createDataFrame([("D1", 100, WEEK_1, 31)], MART_COLUMNS)

    assert load__gold_weekly_mart(spark, df, "test_no_such_table") is False
