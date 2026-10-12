# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: bronze layer
# MAGIC
# MAGIC Creates the catalog, the `bronze` schema and the `bronze.raw_mandi_transactions` table.
# MAGIC
# MAGIC Safe to re-run: every statement uses `IF NOT EXISTS`.
# MAGIC Note: `IF NOT EXISTS` skips an existing table even if its columns differ; it never alters it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "mandi_dev", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

print(f"Setting up bronze layer in catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog

# COMMAND ----------

def create__catalog(catalog: str) -> None:
    """Create the catalog if it does not exist.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".
    """
    print(f"Creating catalog {catalog} (if not exists) ...")
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {catalog}
        COMMENT 'Gujarat mandi price pipeline'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema: bronze

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table: bronze.raw_mandi_transactions

# COMMAND ----------

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

create__catalog(catalog)
print(f"Catalog ready: {catalog}")

create__schema__bronze(catalog)
print(f"Schema ready: {catalog}.bronze")

create__table__raw_mandi_transactions(catalog)
print(f"Table ready: {catalog}.bronze.raw_mandi_transactions")

print("Bronze setup complete")
