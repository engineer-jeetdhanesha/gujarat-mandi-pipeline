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
# MAGIC Raises an error if the load fails, so the run shows as failed.

# COMMAND ----------

dbutils.widgets.text("file_path", "", "File path")
dbutils.widgets.text("run_id", "", "Run id")

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def load__table__raw_mandi_transactions(file_path: str, run_id: str) -> bool:
    """Append the rows of one CSV file to bronze.raw_mandi_transactions.

    The file needs a header row (skipped) followed by the 11 source columns in table
    order. Values are kept as received; metadata columns are added: _ingested_at
    (load time), _source_mode ("FILE"), _file_name (file name without folders),
    _run_id and _row_no (1-based row number in the file, header excluded).

    Args:
        file_path: Path of the CSV file, e.g. "/Volumes/mandi/raw/mandi_2026-01-01.csv".
        run_id: Id of the run loading the file.

    Returns:
        True if rows were loaded; False if the file is empty or the load failed
        (the reason is printed).
    """
    import os

    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    source_mode = "FILE"
    source_columns = [
        "state",
        "district",
        "market",
        "commodity",
        "commodity_code",
        "variety",
        "grade",
        "arrival_date",
        "min_price",
        "max_price",
        "modal_price",
    ]
    schema = StructType([StructField(c, StringType(), True) for c in source_columns])

    try:
        catalog = get__default_catalog()
        if not catalog:
            return False
        table_name = f"{catalog}.bronze.raw_mandi_transactions"

        print(f"Reading {file_path} ...")
        df = (
            spark.read
            .schema(schema)
            .option("header", True)
            .option("mode", "FAILFAST")
            .option("quote", '"')
            .option("escape", '"')
            .csv(file_path)
        )

        # Keep file order: monotonically_increasing_id follows the read order of a single file.
        row_window = Window.orderBy(F.monotonically_increasing_id())
        df = (
            df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_mode", F.lit(source_mode))
            .withColumn("_file_name", F.lit(os.path.basename(file_path)))
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_row_no", F.row_number().over(row_window).cast("bigint"))
        )

        # No cache(): PERSIST is not supported on serverless compute. The file is read twice
        # (count, then write); _ingested_at is set per query, so the write has one value for all rows.
        row_count = df.count()
        if row_count == 0:
            print(f"WARNING: no data rows in {file_path}; nothing loaded")
            return False

        print(f"Appending {row_count} rows to {table_name} (run_id={run_id}) ...")
        df.write.mode("append").saveAsTable(table_name)
        print(f"Loaded {row_count} rows from {os.path.basename(file_path)}")
        return True
    except Exception as e:
        print(f"ERROR: load failed for {file_path}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

file_path = dbutils.widgets.get("file_path").strip()
run_id = dbutils.widgets.get("run_id").strip()

if not file_path or not run_id:
    raise ValueError("Widgets 'file_path' and 'run_id' are required")

if not load__table__raw_mandi_transactions(file_path, run_id):
    raise RuntimeError(f"Bronze load failed for {file_path} (see the error printed above)")

print("Bronze load complete")
