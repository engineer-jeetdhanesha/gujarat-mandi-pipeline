# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: bronze layer
# MAGIC
# MAGIC Creates the `bronze` schema, the `bronze.mandi_raw` table and the
# MAGIC `bronze.raw_files` volume (source CSV files for file loads) in the given catalog.
# MAGIC
# MAGIC Safe to re-run: every statement uses `IF NOT EXISTS`.
# MAGIC Note: `IF NOT EXISTS` skips an existing table even if its columns differ; it never alters it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "gujarat_mandi_pipeline_dev_ws", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema: bronze

# COMMAND ----------

def create__schema__bronze(catalog: str) -> None:
    """Create the bronze schema if it does not exist.

    Args:
        catalog: Unity Catalog name, e.g. "gujarat_mandi_pipeline_dev_ws".
    """
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog}.bronze
        COMMENT 'Raw data stored exactly as received'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table: bronze.mandi_raw

# COMMAND ----------

def create__table__mandi_raw(catalog: str) -> None:
    """Create the bronze.mandi_raw Delta table if it does not exist.

    All source columns are STRING; typing happens in silver.
    Append only; duplicates are removed in silver. No partitioning (small table).

    Args:
        catalog: Unity Catalog name, e.g. "gujarat_mandi_pipeline_dev_ws".
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.mandi_raw (
            State           STRING    COMMENT 'State name',
            District        STRING    COMMENT 'District name',
            Market          STRING    COMMENT 'APMC name',
            Commodity       STRING    COMMENT 'Commodity name',
            Commodity_Code  STRING    COMMENT 'Commodity code',
            Variety         STRING    COMMENT 'Commodity variety',
            Grade           STRING    COMMENT 'Quality grade (e.g. FAQ)',
            Arrival_Date    STRING    COMMENT 'Arrival date, dd/mm/yyyy',
            Min_Price       STRING    COMMENT 'Minimum price (Rs/quintal)',
            Max_Price       STRING    COMMENT 'Maximum price (Rs/quintal)',
            Modal_Price     STRING    COMMENT 'Modal (most common) price (Rs/quintal)',
            _source         STRING    COMMENT 'api or file',
            _source_file    STRING    COMMENT 'CSV path for file loads, NULL for API loads',
            _ingested_at    TIMESTAMP COMMENT 'Time the row was loaded',
            _run_id         STRING    COMMENT 'Databricks job run ID'
        )
        USING DELTA
        COMMENT 'Raw mandi prices from the data.gov.in stored exactly as received'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume: bronze.raw_files

# COMMAND ----------

def create__volume__raw_files(catalog: str) -> None:
    """Create the bronze.raw_files volume if it does not exist.

    Holds the source CSV files read by the file mode of load_bronze_data
    (mandi_*.csv). Managed volume.

    Args:
        catalog: Unity Catalog name, e.g. "gujarat_mandi_pipeline_dev_ws".
    """
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_files
        COMMENT 'Source CSV files for file-mode bronze loads'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

create__schema__bronze(catalog)
print(f"Schema ready: {catalog}.bronze")

create__table__mandi_raw(catalog)
print(f"Table ready: {catalog}.bronze.mandi_raw")

create__volume__raw_files(catalog)
print(f"Volume ready: {catalog}.bronze.raw_files")
