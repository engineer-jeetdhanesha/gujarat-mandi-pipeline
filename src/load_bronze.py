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

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def load__bronze(
    spark: "SparkSession",
    file_path: str,
    schema: "StructType",
    options: dict[str, object],
    run_id: str,
    table_name: str,
) -> bool:
    """Append the rows of one CSV file to the bronze table.

    Args:
        spark: Spark session.
        file_path: Path of the CSV file, e.g. "/Volumes/mandi/raw/mandi_2026-01-01.csv".
        schema: Schema of the file (lib_mandi.schemas.SCHEMA_SOURCE).
        options: Spark read options (lib_mandi.config.OPTIONS_READ_SOURCE).
        run_id: Id of the run loading the file.
        table_name: Three-level table name, e.g. "mandi_dev.bronze.raw_mandi_transactions".

    Returns:
        True if rows were loaded; False if the file has no data rows or the load failed
        (the reason is printed).
    """
    import os

    from lib_mandi.transformations import add__ingest_metadata

    try:
        print(f"Reading {file_path} ...")
        df = spark.read.schema(schema).options(**options).csv(file_path)
        df = add__ingest_metadata(df, run_id, os.path.basename(file_path))

        # No cache(): PERSIST is not supported on serverless compute. The file is read twice
        # (count, then write); _ingested_at is set per query, so the write has one value for all rows.
        row_count = df.count()
        if row_count == 0:
            print(f"WARNING: no data rows in {file_path}; nothing loaded")
            return False

        print(f"Appending {row_count} rows to {table_name} (run_id={run_id}) ...")
        df.write.mode("append").saveAsTable(table_name)
        print(f"Loaded {row_count} rows from {file_path}")
        return True
    except Exception as e:
        print(f"ERROR: load failed for {file_path}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

from lib_mandi.config import OPTIONS_READ_SOURCE
from lib_mandi.schemas import SCHEMA_SOURCE

file_path = dbutils.widgets.get("file_path").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not file_path or not run_id:
    raise ValueError("Widgets 'file_path' and 'run_id' are required")

catalog = get__default_catalog()
if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

load__bronze(
    spark, file_path, SCHEMA_SOURCE, OPTIONS_READ_SOURCE, run_id, f"{catalog}.bronze.raw_mandi_transactions"
)
