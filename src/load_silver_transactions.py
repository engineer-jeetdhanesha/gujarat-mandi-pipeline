# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: bronze to clean and rejected
# MAGIC
# MAGIC Takes the rows of one run from `bronze.raw_mandi_transactions` and checks them against the quality rules.
# MAGIC Rows that pass go to `silver.mandi_prices_clean`; rows that fail go to `silver.mandi_prices_rejected`.
# MAGIC Works in the workspace's default catalog (the tables are created by `src/setup`, the dimensions are loaded by
# MAGIC `src/load_silver_dims`).
# MAGIC
# MAGIC Rules (every failing rule is recorded, in this order, as comma separated ids in `reject_rule`):
# MAGIC - `missing_market`: market is blank.
# MAGIC - `missing_commodity_code`: commodity code is blank.
# MAGIC - `invalid_date`: arrival date is blank or not `dd/MM/yyyy`.
# MAGIC - `invalid_price`: a price is blank or not a number.
# MAGIC - `price_le_zero`: a price is zero or negative.
# MAGIC - `min_gt_max`: min price is greater than max price.
# MAGIC - `modal_out_of_range`: modal price is below min or above max (checked only when min is not above max).
# MAGIC - `unknown_market`: market is not in `silver.dim_market`.
# MAGIC - `unknown_commodity`: commodity code is not in `silver.dim_commodity`.
# MAGIC - `future_date`: arrival date is after today.
# MAGIC - `unknown_date`: arrival date is not in `silver.dim_date`.
# MAGIC - `duplicate`: another row of the run has the same natural key and wins (latest `_ingested_at`, then highest
# MAGIC   `_row_no`). Only rows that pass every other rule are compared.
# MAGIC
# MAGIC Rules that need a valid value are skipped when the value is missing or invalid, so one bad value gives one reason.
# MAGIC
# MAGIC - Clean is a MERGE on the natural key (market, commodity code, variety, grade, arrival date), so running a day
# MAGIC   again updates its rows instead of duplicating them.
# MAGIC - Rejected rows keep the raw values. They start as `ERROR`, or `REJECTED` for duplicates (nothing to fix).
# MAGIC   They are inserted once per bronze row (run id, file name, row number); running again does not change existing ones.
# MAGIC
# MAGIC Parameters (required):
# MAGIC - `run_id`: id of the run whose bronze rows are processed.
# MAGIC
# MAGIC If the load fails, the reason is printed and the notebook still completes.

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run id")

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def load__silver_transactions(
    spark: "SparkSession",
    run_id: str,
    bronze_table: str,
    dim_market_table: str,
    dim_commodity_table: str,
    dim_date_table: str,
    clean_table: str,
    rejected_table: str,
    schema: "StructType",
    rules: list[tuple[str, str, str]],
) -> bool:
    """Split the bronze rows of one run into silver clean and rejected.

    Args:
        spark: Spark session.
        run_id: Id of the run whose rows in the bronze table are processed.
        bronze_table: Three-level name of bronze.raw_mandi_transactions.
        dim_market_table: Three-level name of silver.dim_market.
        dim_commodity_table: Three-level name of silver.dim_commodity.
        dim_date_table: Three-level name of silver.dim_date.
        clean_table: Three-level name of silver.mandi_prices_clean (MERGE target).
        rejected_table: Three-level name of silver.mandi_prices_rejected (insert-only MERGE target).
        schema: Schema of the source columns (lib_mandi.schemas.SCHEMA_SOURCE).
        rules: Data quality rules (lib_mandi.dq_rules.DQ_RULES).

    Returns:
        True if every bronze row of the run is in clean or rejected afterwards; False if the
        run has no bronze rows, a dimension is empty, the counts do not add up or the load
        failed (the reason is printed).
    """
    from pyspark.sql import functions as F

    from lib_mandi.transformations import split__clean_rejected

    try:
        bronze = spark.table(bronze_table).where(F.col("_run_id") == run_id)
        total = bronze.count()
        if total == 0:
            print(f"WARNING: no rows in {bronze_table} for run_id {run_id}; nothing loaded")
            return False

        dims = [spark.table(name) for name in (dim_market_table, dim_commodity_table, dim_date_table)]
        for name, dim in zip((dim_market_table, dim_commodity_table, dim_date_table), dims):
            if dim.count() == 0:
                print(f"ERROR: {name} is empty; run src/load_silver_dims first")
                return False

        clean_df, rejected_df = split__clean_rejected(bronze, *dims, run_id, schema, rules)

        # No cache(): PERSIST is not supported on serverless compute, so each step reads bronze again.
        clean_count = clean_df.count()
        rejected_count = rejected_df.count()
        if clean_count + rejected_count != total:
            print(f"ERROR: {total} bronze rows but {clean_count} clean + {rejected_count} rejected; nothing loaded")
            return False

        print(f"Run {run_id}: {total} bronze rows, {clean_count} clean, {rejected_count} rejected")
        for row in rejected_df.groupBy("reject_rule").count().orderBy(F.desc("count")).collect():
            print(f"  rejected {row['count']}: {row['reject_rule']}")

        if clean_count > 0:
            clean_df.createOrReplaceTempView("silver_clean_input")
            print(f"Merging {clean_count} rows into {clean_table} ...")
            spark.sql(f"""
                MERGE INTO {clean_table} AS t
                USING silver_clean_input AS s
                ON  t.market = s.market
                AND t.commodity_code = s.commodity_code
                AND t.variety <=> s.variety
                AND t.grade <=> s.grade
                AND t.arrival_date = s.arrival_date
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        if rejected_count > 0:
            rejected_df.createOrReplaceTempView("silver_rejected_input")
            print(f"Inserting new rejected rows into {rejected_table} ...")
            spark.sql(f"""
                MERGE INTO {rejected_table} AS t
                USING silver_rejected_input AS s
                ON  t.run_id = s.run_id
                AND t.file_name <=> s.file_name
                AND t.row_no = s.row_no
                WHEN NOT MATCHED THEN INSERT *
            """)

        print(f"Silver transactions load done for run {run_id}")
        return True
    except Exception as e:
        print(f"ERROR: silver transactions load failed for run {run_id}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

from lib_mandi.dq_rules import DQ_RULES
from lib_mandi.schemas import SCHEMA_SOURCE

run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("Widget 'run_id' is required")

catalog = get__default_catalog()
if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

load__silver_transactions(
    spark,
    run_id,
    f"{catalog}.bronze.raw_mandi_transactions",
    f"{catalog}.silver.dim_market",
    f"{catalog}.silver.dim_commodity",
    f"{catalog}.silver.dim_date",
    f"{catalog}.silver.mandi_prices_clean",
    f"{catalog}.silver.mandi_prices_rejected",
    SCHEMA_SOURCE,
    DQ_RULES,
)
