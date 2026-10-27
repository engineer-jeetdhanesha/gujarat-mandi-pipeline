"""Create the schemas and tables of the pipeline. Safe to re-run: every statement uses IF NOT EXISTS.

IF NOT EXISTS skips an existing table even if its columns differ; it never alters it. Each function takes
the catalog and schema as parameters (only the table name is fixed), handles its own errors, prints the
reason and returns False.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create__schema(spark: SparkSession, catalog: str, schema: str, comment: str) -> bool:
    """Create a schema if it does not exist.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema name, e.g. "bronze".
        comment: Description stored with the schema.

    Returns:
        True if the schema exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating schema {catalog}.{schema} (if not exists) ...")
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
            COMMENT '{comment}'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create schema {catalog}.{schema}: {e}")
        return False


def create__table__raw_mandi_transactions(spark: SparkSession, catalog: str, schema: str) -> bool:
    """Create the raw_mandi_transactions Delta table if it does not exist.

    All source columns are STRING; typing happens in silver.
    Append only; duplicates are removed in silver. No partitioning.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema the table is created in, e.g. "silver".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.{schema}.raw_mandi_transactions (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.raw_mandi_transactions (
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
        print(f"ERROR: could not create table {catalog}.{schema}.raw_mandi_transactions: {e}")
        return False


def create__table__dim_market(spark: SparkSession, catalog: str, schema: str) -> bool:
    """Create the dim_market Delta table if it does not exist.

    Loaded from seed/dim_market.csv by src/load_silver_dims.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema the table is created in, e.g. "silver".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.{schema}.dim_market (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dim_market (
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
        print(f"ERROR: could not create table {catalog}.{schema}.dim_market: {e}")
        return False


def create__table__dim_commodity(spark: SparkSession, catalog: str, schema: str) -> bool:
    """Create the dim_commodity Delta table if it does not exist.

    Loaded from seed/dim_commodity.csv by src/load_silver_dims.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema the table is created in, e.g. "silver".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.{schema}.dim_commodity (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dim_commodity (
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
        print(f"ERROR: could not create table {catalog}.{schema}.dim_commodity: {e}")
        return False


def create__table__dim_date(spark: SparkSession, catalog: str, schema: str) -> bool:
    """Create the dim_date Delta table if it does not exist.

    Loaded from seed/dim_date.csv by src/load_silver_dims.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema the table is created in, e.g. "silver".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.{schema}.dim_date (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dim_date (
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
        print(f"ERROR: could not create table {catalog}.{schema}.dim_date: {e}")
        return False


def create__table__mandi_prices_clean(spark: SparkSession, catalog: str, schema: str) -> bool:
    """Create the mandi_prices_clean Delta table if it does not exist.

    Typed and standardised rows that passed the quality rules; MERGE target of src/load_silver_transactions.
    Natural key: market + commodity_code + variety + grade + arrival_date.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema the table is created in, e.g. "silver".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.{schema}.mandi_prices_clean (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.mandi_prices_clean (
                state           STRING        COMMENT 'Trimmed',
                district        STRING        COMMENT 'Raw district (trimmed)',
                district_std    STRING        COMMENT 'Standard district name from silver.dim_market',
                market          STRING        COMMENT 'Trimmed market name',
                commodity_code  INT           COMMENT 'Commodity code (natural key of silver.dim_commodity)',
                commodity       STRING        COMMENT 'Standard name from silver.dim_commodity',
                variety         STRING        COMMENT 'Trimmed variety',
                grade           STRING        COMMENT 'Trimmed grade',
                arrival_date    DATE          COMMENT 'Parsed from dd/MM/yyyy',
                min_price       DECIMAL(10,2) COMMENT 'Rs/quintal',
                max_price       DECIMAL(10,2) COMMENT 'Rs/quintal',
                modal_price     DECIMAL(10,2) COMMENT 'Rs/quintal',
                run_id          STRING        COMMENT 'Run that last wrote the row',
                ingested_at     TIMESTAMP     COMMENT 'Bronze ingest time of the row',
                updated_at      TIMESTAMP     COMMENT 'Last MERGE time'
            )
            USING DELTA
            COMMENT 'Typed, standardised, de-duplicated mandi prices that passed the quality rules'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create table {catalog}.{schema}.mandi_prices_clean: {e}")
        return False


def create__table__mandi_prices_rejected(spark: SparkSession, catalog: str, schema: str) -> bool:
    """Create the mandi_prices_rejected Delta table if it does not exist.

    Rows that failed a quality rule, with the raw values kept. run_id, file_name and row_no
    identify the bronze row.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema the table is created in, e.g. "silver".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.{schema}.mandi_prices_rejected (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.mandi_prices_rejected (
                state           STRING    COMMENT 'As received',
                district        STRING    COMMENT 'As received',
                market          STRING    COMMENT 'As received',
                commodity       STRING    COMMENT 'As received',
                commodity_code  STRING    COMMENT 'As received',
                variety         STRING    COMMENT 'As received',
                grade           STRING    COMMENT 'As received',
                arrival_date    STRING    COMMENT 'As received',
                min_price       STRING    COMMENT 'As received',
                max_price       STRING    COMMENT 'As received',
                modal_price     STRING    COMMENT 'As received',
                source_mode     STRING    COMMENT 'API / FILE / BACKFILL',
                file_name       STRING    COMMENT 'Source file (NULL for API)',
                run_id          STRING    COMMENT 'Run that loaded the row into bronze',
                ingested_at     TIMESTAMP COMMENT 'Bronze ingest time',
                row_no          BIGINT    COMMENT 'Row number within the file/API page',
                reject_reason   STRING    COMMENT 'Readable reasons, separated by semicolons',
                reject_rule     STRING    COMMENT 'Rule ids, separated by commas',
                rejected_at     TIMESTAMP COMMENT 'When rejected',
                status          STRING    COMMENT 'ERROR / RESOLVED / REJECTED / LOADED',
                updated_at      TIMESTAMP COMMENT 'Last status change'
            )
            USING DELTA
            COMMENT 'Rows that failed a quality rule, raw values kept, for review'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create table {catalog}.{schema}.mandi_prices_rejected: {e}")
        return False


def create__table__fact_mandi_prices(spark: SparkSession, catalog: str, schema: str) -> bool:
    """Create the fact_mandi_prices Delta table if it does not exist.

    Derived from silver.mandi_prices_clean by src/load_silver_facts; MERGE target on the grain
    market x commodity x variety x grade x date.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema the table is created in, e.g. "silver".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.{schema}.fact_mandi_prices (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fact_mandi_prices (
                market_key     BIGINT        COMMENT 'FK to dim_market',
                commodity_key  BIGINT        COMMENT 'FK to dim_commodity',
                date_key       INT           COMMENT 'FK to dim_date',
                variety        STRING        COMMENT 'Degenerate dimension',
                grade          STRING        COMMENT 'Degenerate dimension',
                min_price      DECIMAL(10,2) COMMENT 'Rs/quintal',
                max_price      DECIMAL(10,2) COMMENT 'Rs/quintal',
                modal_price    DECIMAL(10,2) COMMENT 'Rs/quintal',
                loaded_at      TIMESTAMP     COMMENT 'Fact load time'
            )
            USING DELTA
            COMMENT 'Mandi prices at market x commodity x variety x grade x date, keyed to the dimensions'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create table {catalog}.{schema}.fact_mandi_prices: {e}")
        return False


def create__table__mart_price_weekly(spark: SparkSession, catalog: str, schema: str) -> bool:
    """Create the mart_price_weekly Delta table if it does not exist.

    Built by src/load_gold from silver.fact_mandi_prices and the dimensions;
    one row per district x commodity x week.

    Args:
        spark: Spark session.
        catalog: Unity Catalog name, e.g. "mandi_dev".
        schema: Schema the table is created in, e.g. "silver".

    Returns:
        True if the table exists afterwards; False if creating it failed
        (the reason is printed).
    """
    try:
        print(f"Creating table {catalog}.{schema}.mart_price_weekly (if not exists) ...")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.mart_price_weekly (
                year                 INT           COMMENT 'ISO year of the week',
                week_number          INT           COMMENT 'ISO week number',
                week_start           DATE          COMMENT 'Monday of the week',
                district_name        STRING        COMMENT 'Standard district name',
                commodity_code       INT           COMMENT 'Commodity code',
                commodity_name       STRING        COMMENT 'Standard commodity name',
                category             STRING        COMMENT 'Commodity category',
                min_price            DECIMAL(10,2) COMMENT 'Lowest min price of the week, Rs/quintal',
                max_price            DECIMAL(10,2) COMMENT 'Highest max price of the week, Rs/quintal',
                avg_modal_price      DECIMAL(10,2) COMMENT 'sum_modal_price / price_records, Rs/quintal',
                prev_week_avg_price  DECIMAL(10,2) COMMENT 'avg_modal_price of the week 7 days earlier; NULL if none',
                wow_change_pct       DECIMAL(10,2) COMMENT 'Week-on-week change in percent (12.5 = +12.5%); NULL if no previous week',
                price_records        BIGINT        COMMENT 'Number of price rows in the group',
                sum_modal_price      DECIMAL(18,2) COMMENT 'Sum of modal prices, for exact rollups',
                markets_count        BIGINT        COMMENT 'Distinct markets reporting',
                is_complete_week     BOOLEAN       COMMENT 'False if the week starts before the first or ends after the last loaded date',
                updated_at           TIMESTAMP     COMMENT 'Last MERGE time'
            )
            USING DELTA
            COMMENT 'Weekly prices per district and commodity, with week-on-week change'
        """)
        return True
    except Exception as e:
        print(f"ERROR: could not create table {catalog}.{schema}.mart_price_weekly: {e}")
        return False
