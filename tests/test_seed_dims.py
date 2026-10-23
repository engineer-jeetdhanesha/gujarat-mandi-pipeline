# Databricks notebook source
# MAGIC %md
# MAGIC # Test: seed dimensions
# MAGIC
# MAGIC Runs `src/setup` and `src/seed_dims`, then checks that each dimension table holds exactly the rows of its
# MAGIC seed CSV (`dim_market`, `dim_commodity`, `dim_date`): `EXCEPT ALL` in both directions returns nothing.
# MAGIC `updated_at` is not compared; the CSV does not have it.
# MAGIC
# MAGIC Tests are plain pytest functions run inside this notebook with `ipytest`.
# MAGIC The notebook fails if any test fails, so it can be used as a job task.
# MAGIC
# MAGIC Cleanup: after the tests, the tables and schemas that this run created are dropped.
# MAGIC Existing ones (and their data) are left as they were.

# COMMAND ----------

# MAGIC %pip install pytest ipytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## State before setup
# MAGIC
# MAGIC Recorded before `src/setup` runs, so cleanup only drops what this run creates.

# COMMAND ----------

test_catalog = spark.catalog.currentCatalog()

# Tables and schemas that setup creates, tables first so they are dropped before their schema.
SETUP_TABLES = [
    f"{test_catalog}.silver.dim_market",
    f"{test_catalog}.silver.dim_commodity",
    f"{test_catalog}.silver.dim_date",
    f"{test_catalog}.bronze.raw_mandi_transactions",
]
SETUP_SCHEMAS = [f"{test_catalog}.silver", f"{test_catalog}.bronze"]

tables_existed = {name: spark.catalog.tableExists(name) for name in SETUP_TABLES}
schemas_existed = {
    name: len(spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE '{name.split('.')[1]}'").collect()) == 1
    for name in SETUP_SCHEMAS
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run setup and seed

# COMMAND ----------

# MAGIC %run ../src/setup

# COMMAND ----------

# MAGIC %run ../src/seed_dims

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
    """After the tests, drop the tables and schemas, only those this run created.

    No CASCADE: dropping a schema fails instead of removing anything else in it.
    """
    yield

    for name in SETUP_TABLES:
        if not tables_existed[name]:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
    for name in SETUP_SCHEMAS:
        if not schemas_existed[name]:
            spark.sql(f"DROP SCHEMA IF EXISTS {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: table matches file
# MAGIC
# MAGIC `seeds` and `seed_dir` come from `src/seed_dims`. Both sides are compared as strings:
# MAGIC the CSV as read, the table with its columns cast to STRING (this also checks the casts).

# COMMAND ----------

def read__seed_file(file_name: str):
    """Read a seed CSV with all columns as STRING."""
    return (
        spark.read
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"file:{seed_dir}/{file_name}" if seed_dir.startswith("/Workspace") else f"{seed_dir}/{file_name}")
    )


@pytest.mark.parametrize("file_name, table_name", seeds)
def test__table_matches_seed_file(file_name, table_name):
    """The table has exactly the rows of the seed file (EXCEPT ALL, both ways)."""
    file_df = read__seed_file(file_name)
    table_df = spark.table(table_name).drop("updated_at")
    table_df = table_df.select(*[table_df[c].cast("string").alias(c) for c in table_df.columns])

    assert file_df.count() > 0
    assert file_df.exceptAll(table_df).count() == 0
    assert table_df.exceptAll(file_df).count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
