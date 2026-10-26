# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean prices to fact table
# MAGIC
# MAGIC Loads the rows one run wrote to `silver.mandi_prices_clean` into `silver.fact_mandi_prices`, looking up the
# MAGIC surrogate keys in `silver.dim_market`, `silver.dim_commodity` and `silver.dim_date`. Works in the workspace's
# MAGIC default catalog (the tables are created by `src/setup`).
# MAGIC
# MAGIC - Input: the clean rows whose `run_id` is the given run (rows the run inserted or updated).
# MAGIC - MERGE on the grain (market key, commodity key, date key, variety, grade), so running a day again updates its
# MAGIC   rows instead of duplicating them.
# MAGIC - Fails, loading nothing, if a clean row has no key in a dimension.
# MAGIC
# MAGIC Parameters (required):
# MAGIC - `run_id`: id of the run whose clean rows are loaded.
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

def load__silver_facts(
    spark: "SparkSession",
    run_id: str,
    clean_table: str,
    dim_market_table: str,
    dim_commodity_table: str,
    dim_date_table: str,
    fact_table: str,
) -> bool:
    """MERGE the clean rows of one run into silver.fact_mandi_prices.

    Args:
        spark: Spark session.
        run_id: Id of the run whose rows in the clean table are loaded.
        clean_table: Three-level name of silver.mandi_prices_clean.
        dim_market_table: Three-level name of silver.dim_market.
        dim_commodity_table: Three-level name of silver.dim_commodity.
        dim_date_table: Three-level name of silver.dim_date.
        fact_table: Three-level name of silver.fact_mandi_prices (MERGE target).

    Returns:
        True if the rows were loaded; False if the run has no clean rows, a row has no
        dimension key or the load failed (the reason is printed).
    """
    from pyspark.sql import functions as F

    from lib_mandi.transformations import build__fact, get__rows_missing_keys

    try:
        clean = spark.table(clean_table).where(F.col("run_id") == run_id)
        clean_count = clean.count()
        if clean_count == 0:
            print(f"WARNING: no rows in {clean_table} for run_id {run_id}; nothing loaded")
            return False

        fact_df = build__fact(
            clean, spark.table(dim_market_table), spark.table(dim_commodity_table), spark.table(dim_date_table)
        )

        # No cache(): PERSIST is not supported on serverless compute, so each step reads clean again.
        missing_keys = get__rows_missing_keys(fact_df).count()
        if missing_keys > 0:
            print(f"ERROR: {missing_keys} of {clean_count} clean rows have no dimension key; nothing loaded")
            return False
        fact_count = fact_df.count()
        if fact_count != clean_count:
            print(f"ERROR: a dimension has duplicate keys ({fact_count} fact rows for {clean_count} clean rows); nothing loaded")
            return False

        fact_df.createOrReplaceTempView("silver_fact_input")
        print(f"Merging {clean_count} rows into {fact_table} (run_id={run_id}) ...")
        spark.sql(f"""
            MERGE INTO {fact_table} AS t
            USING silver_fact_input AS s
            ON  t.market_key = s.market_key
            AND t.commodity_key = s.commodity_key
            AND t.date_key = s.date_key
            AND t.variety <=> s.variety
            AND t.grade <=> s.grade
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Silver facts load done for run {run_id}")
        return True
    except Exception as e:
        print(f"ERROR: silver facts load failed for run {run_id}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("Widget 'run_id' is required")

catalog = get__default_catalog()
if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

load__silver_facts(
    spark,
    run_id,
    f"{catalog}.silver.mandi_prices_clean",
    f"{catalog}.silver.dim_market",
    f"{catalog}.silver.dim_commodity",
    f"{catalog}.silver.dim_date",
    f"{catalog}.silver.fact_mandi_prices",
)
