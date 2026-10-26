# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: weekly price mart
# MAGIC
# MAGIC Updates `gold.mart_price_weekly` (one row per district x commodity x week) from `silver.fact_mandi_prices` and
# MAGIC the silver dimensions, for the weeks that one run changed. Works in the workspace's default catalog (the tables
# MAGIC are created by `src/setup`).
# MAGIC
# MAGIC - Incremental: the district x commodity x week groups of the clean rows the run wrote are found first. Those
# MAGIC   weeks are recomputed, plus the week before (its `is_complete_week` can change) and the week after (its
# MAGIC   `prev_week_avg_price` and `wow_change_pct` change). All other rows of the mart are left alone.
# MAGIC - Each recomputed week is aggregated from all its fact rows, not only the run's, then MERGEd on district,
# MAGIC   commodity and week, so running a day again gives the same rows.
# MAGIC - `avg_modal_price` is `sum_modal_price / price_records`, so any rollup of the columns is exact.
# MAGIC - `wow_change_pct` compares with the same district and commodity exactly 7 days earlier; NULL if that week has no
# MAGIC   data. It is a percentage (12.5 means +12.5%).
# MAGIC - `is_complete_week` is false if the week starts before the first loaded date or ends after the last loaded
# MAGIC   date of the fact table.
# MAGIC
# MAGIC Parameters (required):
# MAGIC - `run_id`: id of the run whose clean rows decide which weeks are recomputed.
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

def load__gold_weekly_mart(
    spark: "SparkSession",
    run_id: str,
    clean_table: str,
    fact_table: str,
    dim_market_table: str,
    dim_commodity_table: str,
    dim_date_table: str,
    mart_table: str,
) -> bool:
    """Recompute the weeks a run changed and MERGE them into gold.mart_price_weekly.

    Args:
        spark: Spark session.
        run_id: Id of the run whose rows in the clean table decide the weeks.
        clean_table: Three-level name of silver.mandi_prices_clean.
        fact_table: Three-level name of silver.fact_mandi_prices.
        dim_market_table: Three-level name of silver.dim_market.
        dim_commodity_table: Three-level name of silver.dim_commodity.
        dim_date_table: Three-level name of silver.dim_date.
        mart_table: Three-level name of gold.mart_price_weekly (MERGE target).

    Returns:
        True if the weeks were loaded; False if the run has no clean rows, the fact table is
        empty or the load failed (the reason is printed).
    """
    from pyspark.sql import functions as F

    from lib_mandi.transformations import build__weekly_mart

    try:
        clean = spark.table(clean_table).where(F.col("run_id") == run_id)
        if clean.count() == 0:
            print(f"WARNING: no rows in {clean_table} for run_id {run_id}; nothing loaded")
            return False

        fact = spark.table(fact_table)
        if fact.limit(1).count() == 0:
            print(f"ERROR: {fact_table} is empty; run src/load_silver_facts first")
            return False

        mart_df = build__weekly_mart(
            clean,
            fact,
            spark.table(dim_market_table),
            spark.table(dim_commodity_table),
            spark.table(dim_date_table),
        )

        # No cache(): PERSIST is not supported on serverless compute, so each step reads the fact again.
        row_count = mart_df.count()
        if row_count == 0:
            print(f"WARNING: no fact rows for the weeks of run_id {run_id}; nothing loaded")
            return False

        mart_df.createOrReplaceTempView("gold_mart_input")
        print(f"Merging {row_count} weekly rows into {mart_table} (run_id={run_id}) ...")
        spark.sql(f"""
            MERGE INTO {mart_table} AS t
            USING gold_mart_input AS s
            ON  t.district_name = s.district_name
            AND t.commodity_code = s.commodity_code
            AND t.week_start = s.week_start
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Gold load done for run {run_id}")
        return True
    except Exception as e:
        print(f"ERROR: gold load failed for run {run_id}: {e}")
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

load__gold_weekly_mart(
    spark,
    run_id,
    f"{catalog}.silver.mandi_prices_clean",
    f"{catalog}.silver.fact_mandi_prices",
    f"{catalog}.silver.dim_market",
    f"{catalog}.silver.dim_commodity",
    f"{catalog}.silver.dim_date",
    f"{catalog}.gold.mart_price_weekly",
)
