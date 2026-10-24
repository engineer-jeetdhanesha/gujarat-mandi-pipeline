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
# MAGIC Raises an error if the load fails, so the run shows as failed.

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run id")

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def load__table__mart_price_weekly(run_id: str) -> bool:
    """Recompute the weeks a run changed and MERGE them into gold.mart_price_weekly.

    Args:
        run_id: Id of the run whose rows in silver.mandi_prices_clean decide the weeks.

    Returns:
        True if the weeks were loaded; False if the run has no clean rows, the fact table is
        empty or the load failed (the reason is printed).
    """
    from functools import reduce

    from pyspark.sql import functions as F

    keys = ["district_name", "commodity_code", "week_start"]

    try:
        catalog = get__default_catalog()
        if not catalog:
            return False
        clean_table = f"{catalog}.silver.mandi_prices_clean"
        fact_table = f"{catalog}.silver.fact_mandi_prices"
        mart_table = f"{catalog}.gold.mart_price_weekly"

        clean = spark.table(clean_table).where(F.col("run_id") == run_id)
        if clean.count() == 0:
            print(f"WARNING: no rows in {clean_table} for run_id {run_id}; nothing loaded")
            return False

        # Weeks the run touched, then the weeks to rewrite (week before, same, week after) and the
        # weeks needed to compute them (each also needs its own previous week).
        touched = clean.select(
            F.col("district_std").alias("district_name"),
            "commodity_code",
            F.trunc("arrival_date", "week").alias("week_start"),
        ).distinct()
        rewrite = reduce(
            lambda a, b: a.union(b),
            [touched.withColumn("week_start", F.date_add("week_start", days)) for days in (-7, 0, 7)],
        ).distinct()
        needed = rewrite.union(rewrite.withColumn("week_start", F.date_add("week_start", -7))).distinct()

        dim_market = spark.table(f"{catalog}.silver.dim_market").select("market_key", "district_name")
        dim_commodity = spark.table(f"{catalog}.silver.dim_commodity").select(
            "commodity_key", "commodity_code", "commodity_name", "category"
        )
        dim_date = spark.table(f"{catalog}.silver.dim_date").select("date_key", F.col("date").alias("price_date"), "week_start")

        detail = (
            spark.table(fact_table)
            .join(F.broadcast(dim_market), "market_key")
            .join(F.broadcast(dim_commodity), "commodity_key")
            .join(F.broadcast(dim_date), "date_key")
        )

        # First and last date loaded in the fact table, for is_complete_week.
        bounds = detail.agg(F.min("price_date").alias("first_date"), F.max("price_date").alias("last_date")).first()
        if bounds["first_date"] is None:
            print(f"ERROR: {fact_table} is empty; run src/fact first")
            return False

        weekly = (
            detail
            .join(needed, keys, "left_semi")
            .groupBy(*keys, "commodity_name", "category")
            .agg(
                F.min("min_price").alias("min_price"),
                F.max("max_price").alias("max_price"),
                F.sum("modal_price").alias("sum_modal_price"),
                F.count(F.lit(1)).alias("price_records"),
                F.countDistinct("market_key").alias("markets_count"),
            )
            .withColumn("avg_modal_price", F.round(F.col("sum_modal_price") / F.col("price_records"), 2))
        )
        previous = weekly.select(
            "district_name",
            "commodity_code",
            F.date_add("week_start", 7).alias("week_start"),
            F.col("avg_modal_price").alias("prev_week_avg_price"),
        )

        mart_df = (
            weekly
            .join(previous, keys, "left")
            .join(rewrite, keys, "left_semi")
            .select(
                F.year(F.date_add("week_start", 3)).alias("year"),
                F.weekofyear("week_start").alias("week_number"),
                "week_start",
                "district_name",
                "commodity_code",
                "commodity_name",
                "category",
                F.col("min_price").cast("decimal(10,2)").alias("min_price"),
                F.col("max_price").cast("decimal(10,2)").alias("max_price"),
                F.col("avg_modal_price").cast("decimal(10,2)").alias("avg_modal_price"),
                F.col("prev_week_avg_price").cast("decimal(10,2)").alias("prev_week_avg_price"),
                F.when(
                    F.col("prev_week_avg_price") > 0,
                    F.round((F.col("avg_modal_price") - F.col("prev_week_avg_price")) / F.col("prev_week_avg_price") * 100, 2),
                ).cast("decimal(10,2)").alias("wow_change_pct"),
                F.col("price_records").cast("bigint").alias("price_records"),
                F.col("sum_modal_price").cast("decimal(18,2)").alias("sum_modal_price"),
                F.col("markets_count").cast("bigint").alias("markets_count"),
                (
                    (F.col("week_start") >= F.lit(bounds["first_date"]))
                    & (F.date_add("week_start", 6) <= F.lit(bounds["last_date"]))
                ).alias("is_complete_week"),
                F.current_timestamp().alias("updated_at"),
            )
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

if not load__table__mart_price_weekly(run_id):
    raise RuntimeError(f"Gold load failed for run {run_id} (see the error printed above)")

print("Gold load complete")
