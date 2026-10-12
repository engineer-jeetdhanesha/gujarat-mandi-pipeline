# Databricks notebook source
# MAGIC %md
# MAGIC # Test: setup
# MAGIC
# MAGIC Runs `src/setup`, then checks the default catalog, the `bronze` schema and the
# MAGIC `bronze.raw_mandi_transactions` table match `docs/table-structure.md`.
# MAGIC
# MAGIC Tests are plain pytest functions run inside this notebook with `ipytest`.
# MAGIC The notebook fails if any test fails, so it can be used as a job task.
# MAGIC
# MAGIC Cleanup: after the tests, the table and the `bronze` schema are dropped,
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## State before setup
# MAGIC
# MAGIC Recorded before `src/setup` runs, so cleanup only drops what this run creates.

# COMMAND ----------

test_catalog = spark.catalog.currentCatalog()
TABLE_NAME = f"{test_catalog}.bronze.raw_mandi_transactions"

schema_existed = len(spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'bronze'").collect()) == 1
table_existed = spark.catalog.tableExists(TABLE_NAME)

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

# Keep pytest from writing __pycache__ into the Git folder.
sys.dont_write_bytecode = True

ipytest.autoconfig(raise_on_error=True, addopts=["-p", "no:cacheprovider"])

# COMMAND ----------

@pytest.fixture(scope="module", autouse=True)
def drop_created_objects():
    """After the tests, drop the table and schema, only if this run created them.

    No CASCADE: dropping the schema fails instead of removing anything else in it.
    """
    yield

    if not table_existed:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    if not schema_existed:
        spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: default catalog

# COMMAND ----------

def test__get_default_catalog():
    """get__default_catalog returns the workspace's current catalog name."""
    name = get__default_catalog()
    assert isinstance(name, str) and name
    assert name == spark.catalog.currentCatalog()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: bronze schema

# COMMAND ----------

def test__create_schema_bronze():
    """The bronze schema exists in the default catalog."""
    schemas = spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'bronze'").collect()
    assert len(schemas) == 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: bronze.raw_mandi_transactions

# COMMAND ----------

def test__create_table_raw_mandi_transactions():
    """The bronze.raw_mandi_transactions table exists."""
    assert spark.catalog.tableExists(TABLE_NAME)


def test__table_columns_and_types():
    """Source columns are all STRING; metadata columns keep their own types."""
    fields = spark.table(TABLE_NAME).schema.fields
    actual = [(f.name, f.dataType.simpleString()) for f in fields]
    assert actual == EXPECTED_COLUMNS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
