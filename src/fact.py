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
# MAGIC Raises an error if the load fails, so the run shows as failed.

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run id")

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def load__table__fact_mandi_prices(run_id: str) -> bool:
    """MERGE the clean rows of one run into silver.fact_mandi_prices.

    Args:
        run_id: Id of the run whose rows in silver.mandi_prices_clean are loaded.

    Returns:
        True if the rows were loaded; False if the run has no clean rows, a row has no
        dimension key or the load failed (the reason is printed).
    """
    from pyspark.sql import functions as F

    try:
        catalog = get__default_catalog()
        if not catalog:
            return False
        clean_table = f"{catalog}.silver.mandi_prices_clean"
        fact_table = f"{catalog}.silver.fact_mandi_prices"

        clean = spark.table(clean_table).where(F.col("run_id") == run_id)
        clean_count = clean.count()
        if clean_count == 0:
            print(f"WARNING: no rows in {clean_table} for run_id {run_id}; nothing loaded")
            return False

        dim_market = spark.table(f"{catalog}.silver.dim_market").select("market_key", "market_name")
        dim_commodity = spark.table(f"{catalog}.silver.dim_commodity").select("commodity_key", "commodity_code")
        dim_date = spark.table(f"{catalog}.silver.dim_date").select("date_key", F.col("date").alias("dim_date"))

        fact_df = (
            clean
            .join(F.broadcast(dim_market), clean["market"] == dim_market["market_name"], "left")
            .join(F.broadcast(dim_commodity), clean["commodity_code"] == dim_commodity["commodity_code"], "left")
            .join(F.broadcast(dim_date), clean["arrival_date"] == dim_date["dim_date"], "left")
            .select(
                F.col("market_key"),
                F.col("commodity_key"),
                F.col("date_key"),
                F.col("variety"),
                F.col("grade"),
                F.col("min_price"),
                F.col("max_price"),
                F.col("modal_price"),
                F.current_timestamp().alias("loaded_at"),
            )
        )

        # No cache(): PERSIST is not supported on serverless compute, so each step reads clean again.
        missing_keys = fact_df.where(
            F.col("market_key").isNull() | F.col("commodity_key").isNull() | F.col("date_key").isNull()
        ).count()
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
        print(f"Fact load done for run {run_id}")
        return True
    except Exception as e:
        print(f"ERROR: fact load failed for run {run_id}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("Widget 'run_id' is required")

if not load__table__fact_mandi_prices(run_id):
    raise RuntimeError(f"Fact load failed for run {run_id} (see the error printed above)")

print("Fact load complete")
