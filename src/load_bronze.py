# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: load one mandi file
# MAGIC
# MAGIC Loads one CSV file into `bronze.raw_mandi_transactions` in the workspace's default catalog
# MAGIC (the table is created by `src/setup`).
# MAGIC
# MAGIC - Source columns are loaded exactly as received (all STRING, no trimming or cleaning).
# MAGIC - Metadata columns are derived here: `_ingested_at`, `_source_mode` (`FILE`), `_file_name`, `_run_id`, `_row_no`.
# MAGIC - Append only: loading the same file twice adds its rows twice. Duplicates are removed in silver.
# MAGIC
# MAGIC Parameters (both required):
# MAGIC - `file_path`: path of the CSV file to load.
# MAGIC - `run_id`: id of the run loading the file.
# MAGIC
# MAGIC If the load fails, the reason is printed and the notebook still completes.

# COMMAND ----------

dbutils.widgets.text("file_path", "", "File path")
dbutils.widgets.text("run_id", "", "Run id")

# COMMAND ----------

import os

from lib_mandi.config import OPTIONS_READ_SOURCE, SCHEMA_SOURCE
from lib_mandi.extract import extract__bronze
from lib_mandi.load import load__bronze
from lib_mandi.transformations import transform__bronze
from lib_mandi.utility import get__default_catalog

file_path = dbutils.widgets.get("file_path").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not file_path or not run_id:
    raise ValueError("Widgets 'file_path' and 'run_id' are required")

catalog = get__default_catalog(spark)
if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

df = extract__bronze(spark, file_path, SCHEMA_SOURCE, OPTIONS_READ_SOURCE)
if df is not None:
    df = transform__bronze(df, run_id, os.path.basename(file_path))
if df is not None:
    load__bronze(df, f"{catalog}.bronze.raw_mandi_transactions")
