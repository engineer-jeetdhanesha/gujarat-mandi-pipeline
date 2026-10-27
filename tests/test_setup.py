# Databricks notebook source
# MAGIC %md
# MAGIC # Test: setup
# MAGIC
# MAGIC Runs `src/setup`, then checks the default catalog, the `bronze`, `silver` and `gold` schemas, the
# MAGIC `bronze.raw_mandi_transactions` table, the six silver tables and the gold mart match `docs/table-structure.md`.
# MAGIC
# MAGIC Tests are plain pytest functions run inside this notebook with `ipytest`.
# MAGIC The notebook fails if any test fails, so it can be used as a job task.
# MAGIC
# MAGIC Cleanup: after the tests, the tables and the `bronze`, `silver` and `gold` schemas are dropped,
# MAGIC but only if this run created them. Existing ones (and their data) are left as they were.

# COMMAND ----------

# MAGIC %pip install pytest ipytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# (name, type) in table order, from docs/table-structure.md.
EXPECTED_COLUMNS = [
    ("state", "string"),
    ("district", "string"),
    ("market", "string"),
    ("commodity", "string"),
    ("commodity_code", "string"),
    ("variety", "string"),
    ("grade", "string"),
    ("arrival_date", "string"),
    ("min_price", "string"),
    ("max_price", "string"),
    ("modal_price", "string"),
    ("_ingested_at", "timestamp"),
    ("_source_mode", "string"),
    ("_file_name", "string"),
    ("_run_id", "string"),
    ("_row_no", "bigint"),
]

# (name, type) in table order, from docs/table-structure.md. updated_at has no default: code sets it.
EXPECTED_SILVER_COLUMNS = {
    "dim_market": [
        ("market_key", "bigint"),
        ("state", "string"),
        ("district_name", "string"),
        ("market_name", "string"),
        ("updated_at", "timestamp"),
    ],
    "dim_commodity": [
        ("commodity_key", "bigint"),
        ("commodity_code", "int"),
        ("commodity_name", "string"),
        ("category", "string"),
        ("is_perishable", "boolean"),
        ("updated_at", "timestamp"),
    ],
    "dim_date": [
        ("date_key", "int"),
        ("date", "date"),
        ("day", "int"),
        ("day_name", "string"),
        ("week_start", "date"),
        ("month", "int"),
        ("month_name", "string"),
        ("quarter", "int"),
        ("year", "int"),
        ("is_weekend", "boolean"),
        ("season", "string"),
        ("updated_at", "timestamp"),
    ],
    "mandi_prices_clean": [
        ("state", "string"),
        ("district", "string"),
        ("district_std", "string"),
        ("market", "string"),
        ("commodity_code", "int"),
        ("commodity", "string"),
        ("variety", "string"),
        ("grade", "string"),
        ("arrival_date", "date"),
        ("min_price", "decimal(10,2)"),
        ("max_price", "decimal(10,2)"),
        ("modal_price", "decimal(10,2)"),
        ("run_id", "string"),
        ("ingested_at", "timestamp"),
        ("updated_at", "timestamp"),
    ],
    "fact_mandi_prices": [
        ("market_key", "bigint"),
        ("commodity_key", "bigint"),
        ("date_key", "int"),
        ("variety", "string"),
        ("grade", "string"),
        ("min_price", "decimal(10,2)"),
        ("max_price", "decimal(10,2)"),
        ("modal_price", "decimal(10,2)"),
        ("loaded_at", "timestamp"),
    ],
    "mandi_prices_rejected": [
        ("state", "string"),
        ("district", "string"),
        ("market", "string"),
        ("commodity", "string"),
        ("commodity_code", "string"),
        ("variety", "string"),
        ("grade", "string"),
        ("arrival_date", "string"),
        ("min_price", "string"),
        ("max_price", "string"),
        ("modal_price", "string"),
        ("source_mode", "string"),
        ("file_name", "string"),
        ("run_id", "string"),
        ("ingested_at", "timestamp"),
        ("row_no", "bigint"),
        ("reject_reason", "string"),
        ("reject_rule", "string"),
        ("rejected_at", "timestamp"),
        ("status", "string"),
        ("updated_at", "timestamp"),
    ],
}

# (name, type) in table order, from docs/table-structure.md.
EXPECTED_GOLD_COLUMNS = {
    "mart_price_weekly": [
        ("year", "int"),
        ("week_number", "int"),
        ("week_start", "date"),
        ("district_name", "string"),
        ("commodity_code", "int"),
        ("commodity_name", "string"),
        ("category", "string"),
        ("min_price", "decimal(10,2)"),
        ("max_price", "decimal(10,2)"),
        ("avg_modal_price", "decimal(10,2)"),
        ("prev_week_avg_price", "decimal(10,2)"),
        ("wow_change_pct", "decimal(10,2)"),
        ("price_records", "bigint"),
        ("sum_modal_price", "decimal(18,2)"),
        ("markets_count", "bigint"),
        ("is_complete_week", "boolean"),
        ("updated_at", "timestamp"),
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## State before setup
# MAGIC
# MAGIC Recorded before `src/setup` runs, so cleanup only drops what this run creates.

# COMMAND ----------

test_catalog = spark.catalog.currentCatalog()
TABLE_NAME = f"{test_catalog}.bronze.raw_mandi_transactions"

SILVER_TABLES = {name: f"{test_catalog}.silver.{name}" for name in EXPECTED_SILVER_COLUMNS}

schema_existed = len(spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'bronze'").collect()) == 1
table_existed = spark.catalog.tableExists(TABLE_NAME)
silver_schema_existed = len(spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'silver'").collect()) == 1
silver_tables_existed = {name: spark.catalog.tableExists(full) for name, full in SILVER_TABLES.items()}
GOLD_TABLES = {name: f"{test_catalog}.gold.{name}" for name in EXPECTED_GOLD_COLUMNS}
gold_schema_existed = len(spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'gold'").collect()) == 1
gold_tables_existed = {name: spark.catalog.tableExists(full) for name, full in GOLD_TABLES.items()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run setup

# COMMAND ----------

# MAGIC %run ../src/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## pytest setup

# COMMAND ----------

import sys

import ipytest
import pytest

from lib_mandi.utility import get__default_catalog

# Keep pytest from writing __pycache__ into the Git folder.
sys.dont_write_bytecode = True

ipytest.autoconfig(raise_on_error=True, addopts=["-p", "no:cacheprovider"])

# COMMAND ----------

@pytest.fixture(scope="module", autouse=True)
def drop_created_objects() -> "Generator[None, None, None]":
    """After the tests, drop the tables and schemas, only those this run created.

    No CASCADE: dropping a schema fails instead of removing anything else in it.
    """
    yield

    if not table_existed:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    for name, full in GOLD_TABLES.items():
        if not gold_tables_existed[name]:
            spark.sql(f"DROP TABLE IF EXISTS {full}")
    for name, full in SILVER_TABLES.items():
        if not silver_tables_existed[name]:
            spark.sql(f"DROP TABLE IF EXISTS {full}")
    if not schema_existed:
        spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.bronze")
    if not gold_schema_existed:
        spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.gold")
    if not silver_schema_existed:
        spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: default catalog

# COMMAND ----------

def test__get_default_catalog() -> None:
    """get__default_catalog returns the workspace's current catalog name."""
    name = get__default_catalog(spark)
    assert isinstance(name, str) and name
    assert name == spark.catalog.currentCatalog()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: bronze schema

# COMMAND ----------

def test__create_schema_bronze() -> None:
    """The bronze schema exists in the default catalog."""
    schemas = spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'bronze'").collect()
    assert len(schemas) == 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: bronze.raw_mandi_transactions

# COMMAND ----------

def test__create_table_raw_mandi_transactions() -> None:
    """The bronze.raw_mandi_transactions table exists."""
    assert spark.catalog.tableExists(TABLE_NAME)


def test__table_columns_and_types() -> None:
    """Source columns are all STRING; metadata columns keep their own types."""
    fields = spark.table(TABLE_NAME).schema.fields
    actual = [(f.name, f.dataType.simpleString()) for f in fields]
    assert actual == EXPECTED_COLUMNS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: silver schema

# COMMAND ----------

def test__create_schema_silver() -> None:
    """The silver schema exists in the default catalog."""
    schemas = spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'silver'").collect()
    assert len(schemas) == 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: silver tables

# COMMAND ----------

@pytest.mark.parametrize("table", list(EXPECTED_SILVER_COLUMNS))
def test__create_silver_table(table: str) -> None:
    """Each silver table exists."""
    assert spark.catalog.tableExists(SILVER_TABLES[table])


@pytest.mark.parametrize("table", list(EXPECTED_SILVER_COLUMNS))
def test__silver_table_columns_and_types(table: str) -> None:
    """Each silver table has the columns and types of docs/table-structure.md, in order."""
    fields = spark.table(SILVER_TABLES[table]).schema.fields
    actual = [(f.name, f.dataType.simpleString()) for f in fields]
    assert actual == EXPECTED_SILVER_COLUMNS[table]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: gold schema and tables

# COMMAND ----------

def test__create_schema_gold() -> None:
    """The gold schema exists in the default catalog."""
    schemas = spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'gold'").collect()
    assert len(schemas) == 1


@pytest.mark.parametrize("table", list(EXPECTED_GOLD_COLUMNS))
def test__gold_table_columns_and_types(table: str) -> None:
    """Each gold table exists and has the columns and types of docs/table-structure.md, in order."""
    assert spark.catalog.tableExists(GOLD_TABLES[table])
    fields = spark.table(GOLD_TABLES[table]).schema.fields
    actual = [(f.name, f.dataType.simpleString()) for f in fields]
    assert actual == EXPECTED_GOLD_COLUMNS[table]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
