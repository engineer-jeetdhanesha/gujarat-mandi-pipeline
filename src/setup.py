# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: bronze and silver dimensions
# MAGIC
# MAGIC Creates the `bronze` schema and the `bronze.raw_mandi_transactions` table, then the `silver` schema and the
# MAGIC `silver.dim_market`, `silver.dim_commodity` and `silver.dim_date` tables (loaded by `src/seed_dims`),
# MAGIC in the workspace's default catalog (`SELECT current_catalog()`), which must already exist.
# MAGIC
# MAGIC Safe to re-run: every statement uses `IF NOT EXISTS`.
# MAGIC Note: `IF NOT EXISTS` skips an existing table even if its columns differ; it never alters it.
# MAGIC
# MAGIC Raises an error if any step fails, so the run shows as failed.

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def create__schema__bronze(catalog: str) -> bool:
    """Create the bronze schema if it does not exist.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".

    Returns:
        True if the schema exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating schema {catalog}.bronze (if not exists) ...")
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog}.bronze
            COMMENT 'Raw data stored exactly as received'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create schema {catalog}.bronze: {e}")
        return False

# COMMAND ----------

def create__table__raw_mandi_transactions(catalog: str) -> bool:
    """Create the bronze.raw_mandi_transactions Delta table if it does not exist.

    All source columns are STRING; typing happens in silver.
    Append only; duplicates are removed in silver. No partitioning.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
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
        return True
    except Exception as e:
        print(f"ERROR: could not create table {catalog}.bronze.raw_mandi_transactions: {e}")
        return False

# COMMAND ----------

def create__schema__silver(catalog: str) -> bool:
    """Create the silver schema if it does not exist.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".

    Returns:
        True if the schema exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating schema {catalog}.silver (if not exists) ...")
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog}.silver
            COMMENT 'Cleaned, typed and standardised data, dimensions and facts'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create schema {catalog}.silver: {e}")
        return False

# COMMAND ----------

def create__table__dim_market(catalog: str) -> bool:
    """Create the silver.dim_market Delta table if it does not exist.

    Loaded from seed/dim_market.csv by src/seed_dims.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.silver.dim_market (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_market (
                market_key     BIGINT    COMMENT 'Surrogate key',
                state          STRING    COMMENT 'Gujarat',
                district_name  STRING    COMMENT 'Standard district name',
                market_name    STRING    COMMENT 'Market (APMC) name, natural key',
                updated_at     TIMESTAMP COMMENT 'Last seed load'
            )
            USING DELTA
            COMMENT 'Market dimension, one row per APMC market'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create table {catalog}.silver.dim_market: {e}")
        return False

# COMMAND ----------

def create__table__dim_commodity(catalog: str) -> bool:
    """Create the silver.dim_commodity Delta table if it does not exist.

    Loaded from seed/dim_commodity.csv by src/seed_dims.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.silver.dim_commodity (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_commodity (
                commodity_key   BIGINT    COMMENT 'Surrogate key',
                commodity_code  INT       COMMENT 'Agmarknet commodity code, natural key',
                commodity_name  STRING    COMMENT 'Standard commodity name',
                category        STRING    COMMENT 'Cereal / pulse / oilseed / vegetable / fruit / spice / cash crop',
                is_perishable   BOOLEAN   COMMENT 'True for fresh produce that spoils quickly',
                updated_at      TIMESTAMP COMMENT 'Last seed load'
            )
            USING DELTA
            COMMENT 'Commodity dimension, one row per commodity code'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create table {catalog}.silver.dim_commodity: {e}")
        return False

# COMMAND ----------

def create__table__dim_date(catalog: str) -> bool:
    """Create the silver.dim_date Delta table if it does not exist.

    Loaded from seed/dim_date.csv by src/seed_dims.

    Args:
        catalog: Unity Catalog name, e.g. "mandi_dev".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.silver.dim_date (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_date (
                date_key    INT       COMMENT 'yyyyMMdd',
                date        DATE      COMMENT 'Calendar date, natural key',
                day         INT       COMMENT 'Day of month',
                day_name    STRING    COMMENT 'Monday ... Sunday',
                week_start  DATE      COMMENT 'Monday of the week',
                month       INT       COMMENT 'Month number',
                month_name  STRING    COMMENT 'January ... December',
                quarter     INT       COMMENT 'Calendar quarter 1-4',
                year        INT       COMMENT 'Calendar year',
                is_weekend  BOOLEAN   COMMENT 'Saturday or Sunday',
                season      STRING    COMMENT 'Kharif (Jun-Oct) / Rabi (Nov-Mar) / Zaid (Apr-May)',
                updated_at  TIMESTAMP COMMENT 'Last seed load'
            )
            USING DELTA
            COMMENT 'Date dimension, one row per calendar day'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create table {catalog}.silver.dim_date: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

catalog = get__default_catalog()
print(f"Setting up bronze and silver dimensions in catalog: {catalog}")

if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

if not create__schema__bronze(catalog):
    raise RuntimeError(f"Could not create schema {catalog}.bronze (see the error printed above)")
print(f"Schema ready: {catalog}.bronze")

if not create__table__raw_mandi_transactions(catalog):
    raise RuntimeError(f"Could not create table {catalog}.bronze.raw_mandi_transactions (see the error printed above)")
print(f"Table ready: {catalog}.bronze.raw_mandi_transactions")

if not create__schema__silver(catalog):
    raise RuntimeError(f"Could not create schema {catalog}.silver (see the error printed above)")
print(f"Schema ready: {catalog}.silver")

for create__table in (create__table__dim_market, create__table__dim_commodity, create__table__dim_date):
    table = create__table.__name__.replace("create__table__", "")
    if not create__table(catalog):
        raise RuntimeError(f"Could not create table {catalog}.silver.{table} (see the error printed above)")
    print(f"Table ready: {catalog}.silver.{table}")

print("Setup complete")
