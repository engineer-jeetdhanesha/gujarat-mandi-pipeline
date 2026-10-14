# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: bronze layer
# MAGIC
# MAGIC Creates the `bronze` schema and the `bronze.raw_mandi_transactions` table in the workspace's default catalog
# MAGIC (`SELECT current_catalog()`), which must already exist.
# MAGIC
# MAGIC Safe to re-run: every statement uses `IF NOT EXISTS`.
# MAGIC Note: `IF NOT EXISTS` skips an existing table even if its columns differ; it never alters it.
# MAGIC
# MAGIC Exits with `SUCCESS` or `FAILURE: <message>`.

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def create__schema__bronze(catalog: str) -> None:
    """Create the bronze schema if it does not exist.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".
    """
    print(f"Creating schema {catalog}.bronze (if not exists) ...")
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog}.bronze
        COMMENT 'Raw data stored exactly as received'
    """)


def create__table__raw_mandi_transactions(catalog: str) -> None:
    """Create the bronze.raw_mandi_transactions Delta table if it does not exist.

    All source columns are STRING; typing happens in silver.
    Append only; duplicates are removed in silver. No partitioning.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".
    """
    print(f"Creating table {catalog}.bronze.raw_mandi_transactions (if not exists) ...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.raw_mandi_transactions (
            state           STRING    COMMENT 'As received (always Gujarat)',
            district        STRING    COMMENT 'Raw district spelling (e.g. Banaskanth)',
            market          STRING    COMMENT 'APMC yard name, raw',
            commodity       STRING    COMMENT 'Commodity name, raw',
            commodity_code  STRING    COMMENT 'Agmarknet commodity code',
            variety         STRING    COMMENT 'Variety, raw ("Other" is common)',
            grade           STRING    COMMENT 'Grade, raw (FAQ, Local, Medium...)',
            arrival_date    STRING    COMMENT 'dd/MM/yyyy as received',
            min_price       STRING    COMMENT 'Rs/quintal, raw (may be decimal)',
            max_price       STRING    COMMENT 'Rs/quintal, raw',
            modal_price     STRING    COMMENT 'Rs/quintal, raw',
            _ingested_at    TIMESTAMP COMMENT 'Load time',
            _source_mode    STRING    COMMENT 'API / FILE / BACKFILL',
            _file_name      STRING    COMMENT 'Source file (NULL for API)',
            _run_id         STRING    COMMENT 'Run that loaded the row (joins audit)',
            _row_no         BIGINT    COMMENT 'Row number within the file/API page'
        )
        USING DELTA
        COMMENT 'Raw, append-only mandi prices stored exactly as received'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

try:
    catalog = get__default_catalog()
    print(f"Setting up bronze layer in catalog: {catalog}")

    create__schema__bronze(catalog)
    print(f"Schema ready: {catalog}.bronze")

    create__table__raw_mandi_transactions(catalog)
    print(f"Table ready: {catalog}.bronze.raw_mandi_transactions")

    print("Bronze setup complete")
except Exception as e:
    print(f"ERROR: bronze setup failed: {e}")
    dbutils.notebook.exit(get__exit_message(False, str(e)))

dbutils.notebook.exit(get__exit_message(True))
