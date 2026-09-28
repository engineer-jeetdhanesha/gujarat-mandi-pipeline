# Databricks notebook source
# MAGIC %md
# MAGIC # Test: setup_bronze
# MAGIC
# MAGIC Runs `src/setup/setup_bronze` against `catalog`, then checks the bronze schema and
# MAGIC the `bronze.mandi_raw` table match `docs/table_details.md`.
# MAGIC
# MAGIC Tests are plain pytest functions run inside this notebook with `ipytest`.
# MAGIC The notebook fails if any test fails, so it can be used as a job task.
# MAGIC
# MAGIC Nothing is dropped afterwards: `setup_bronze` is idempotent and the target may be a shared catalog.

# COMMAND ----------

# MAGIC %pip install pytest ipytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "gujarat_mandi_pipeline_dev_ws", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

SETUP_BRONZE_NOTEBOOK = "../../src/setup/setup_bronze"
TABLE_NAME = f"{catalog}.bronze.mandi_raw"

# Max seconds setup_bronze may run before it is failed.
NOTEBOOK_TIMEOUT_SECONDS = 600

# (name, type) in table order, from docs/table_details.md.
EXPECTED_COLUMNS = [
    ("State", "string"),
    ("District", "string"),
    ("Market", "string"),
    ("Commodity", "string"),
    ("Commodity_Code", "string"),
    ("Variety", "string"),
    ("Grade", "string"),
    ("Arrival_Date", "string"),
    ("Min_Price", "string"),
    ("Max_Price", "string"),
    ("Modal_Price", "string"),
    ("_source", "string"),
    ("_source_file", "string"),
    ("_ingested_at", "timestamp"),
    ("_run_id", "string"),
]

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
def run_setup_bronze():
    """Run setup_bronze once before the tests."""
    dbutils.notebook.run(SETUP_BRONZE_NOTEBOOK, NOTEBOOK_TIMEOUT_SECONDS, {"catalog": catalog})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests: schema

# COMMAND ----------

def test__schema__bronze_exists():
    """The bronze schema exists in the catalog."""
    schemas = spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE 'bronze'").collect()
    assert len(schemas) == 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests: bronze.mandi_raw

# COMMAND ----------

def test__table__mandi_raw_exists():
    """The bronze.mandi_raw table exists."""
    assert spark.catalog.tableExists(TABLE_NAME)


def test__table__mandi_raw_is_delta():
    """bronze.mandi_raw is a Delta table."""
    detail = spark.sql(f"DESCRIBE DETAIL {TABLE_NAME}").first()
    assert detail["format"] == "delta"


def test__table__mandi_raw_columns():
    """Column names, order and types match the data model."""
    fields = spark.table(TABLE_NAME).schema.fields
    actual = [(f.name, f.dataType.simpleString()) for f in fields]
    assert actual == EXPECTED_COLUMNS


def test__table__mandi_raw_column_comments():
    """Every column has a non-empty comment."""
    fields = spark.table(TABLE_NAME).schema.fields
    missing = [f.name for f in fields if not f.metadata.get("comment")]
    assert missing == []


def test__table__mandi_raw_table_comment():
    """The table has a non-empty comment."""
    detail = spark.sql(f"DESCRIBE DETAIL {TABLE_NAME}").first()
    assert detail["description"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests: idempotency

# COMMAND ----------

def test__setup_bronze__rerun_is_idempotent():
    """Running setup_bronze again succeeds and leaves the table unchanged."""
    schema_before = spark.table(TABLE_NAME).schema
    dbutils.notebook.run(SETUP_BRONZE_NOTEBOOK, NOTEBOOK_TIMEOUT_SECONDS, {"catalog": catalog})
    assert spark.table(TABLE_NAME).schema == schema_before

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
