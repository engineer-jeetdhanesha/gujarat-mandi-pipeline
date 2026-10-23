# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: bronze to clean and rejected
# MAGIC
# MAGIC Takes the rows of one run from `bronze.raw_mandi_transactions` and checks them against the quality rules.
# MAGIC Rows that pass go to `silver.mandi_prices_clean`; rows that fail go to `silver.mandi_prices_rejected`.
# MAGIC Works in the workspace's default catalog (the tables are created by `src/setup`, the dimensions are loaded by
# MAGIC `src/seed_dims`).
# MAGIC
# MAGIC Rules (every failing rule is recorded, in this order, as comma separated ids in `reject_rule`):
# MAGIC - `missing_market`: market is blank.
# MAGIC - `missing_commodity_code`: commodity code is blank.
# MAGIC - `invalid_date`: arrival date is blank or not `dd/MM/yyyy`.
# MAGIC - `invalid_price`: a price is blank or not a number.
# MAGIC - `price_le_zero`: a price is zero or negative.
# MAGIC - `min_gt_max`: min price is greater than max price.
# MAGIC - `modal_out_of_range`: modal price is below min or above max.
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
# MAGIC Raises an error if the load fails, so the run shows as failed.

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run id")

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def load__tables__mandi_prices(run_id: str) -> bool:
    """Split the bronze rows of one run into silver clean and rejected.

    Args:
        run_id: Id of the run whose rows in bronze.raw_mandi_transactions are processed.

    Returns:
        True if every bronze row of the run is in clean or rejected afterwards; False if the
        run has no bronze rows, a dimension is empty, the counts do not add up or the load
        failed (the reason is printed).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source_columns = [
        "state",
        "district",
        "market",
        "commodity",
        "commodity_code",
        "variety",
        "grade",
        "arrival_date",
        "min_price",
        "max_price",
        "modal_price",
    ]

    try:
        catalog = get__default_catalog()
        if not catalog:
            return False
        bronze_table = f"{catalog}.bronze.raw_mandi_transactions"
        clean_table = f"{catalog}.silver.mandi_prices_clean"
        rejected_table = f"{catalog}.silver.mandi_prices_rejected"

        bronze = spark.table(bronze_table).where(F.col("_run_id") == run_id)
        total = bronze.count()
        if total == 0:
            print(f"WARNING: no rows in {bronze_table} for run_id {run_id}; nothing loaded")
            return False

        dim_market = spark.table(f"{catalog}.silver.dim_market").select(
            F.col("market_key").alias("dm_market_key"),
            F.col("market_name").alias("dm_market_name"),
            F.col("district_name").alias("dm_district_name"),
        )
        dim_commodity = spark.table(f"{catalog}.silver.dim_commodity").select(
            F.col("commodity_key").alias("dc_commodity_key"),
            F.col("commodity_code").alias("dc_commodity_code"),
            F.col("commodity_name").alias("dc_commodity_name"),
        )
        dim_date = spark.table(f"{catalog}.silver.dim_date").select(
            F.col("date_key").alias("dd_date_key"),
            F.col("date").alias("dd_date"),
        )
        for name, dim in (("dim_market", dim_market), ("dim_commodity", dim_commodity), ("dim_date", dim_date)):
            if dim.count() == 0:
                print(f"ERROR: {catalog}.silver.{name} is empty; run src/seed_dims first")
                return False

        # Trimmed copies (t_), typed values (d_) and the dimension lookups (dm_, dc_, dd_).
        df = bronze
        for c in source_columns:
            df = df.withColumn(f"t_{c}", F.regexp_replace(F.col(c), r"^\s+|\s+$", ""))
        df = (
            df
            .withColumn(
                "d_date",
                F.when(
                    F.col("t_arrival_date").rlike(r"^\d{2}/\d{2}/\d{4}$"),
                    F.expr("try_to_timestamp(t_arrival_date, 'dd/MM/yyyy')").cast("date"),
                ),
            )
            .withColumn("d_min", F.expr("try_cast(t_min_price AS DECIMAL(10,2))"))
            .withColumn("d_max", F.expr("try_cast(t_max_price AS DECIMAL(10,2))"))
            .withColumn("d_modal", F.expr("try_cast(t_modal_price AS DECIMAL(10,2))"))
            .withColumn("d_code", F.expr("try_cast(t_commodity_code AS INT)"))
            .join(F.broadcast(dim_market), F.col("t_market") == F.col("dm_market_name"), "left")
            .join(F.broadcast(dim_commodity), F.col("d_code") == F.col("dc_commodity_code"), "left")
            .join(F.broadcast(dim_date), F.col("d_date") == F.col("dd_date"), "left")
        )

        def is_blank(column: str):
            return F.col(column).isNull() | (F.col(column) == "")

        prices_ok = F.col("d_min").isNotNull() & F.col("d_max").isNotNull() & F.col("d_modal").isNotNull()

        # (rule id, reason, condition), in the order the ids are listed.
        rules = [
            ("missing_market", "Market is missing", is_blank("t_market")),
            ("missing_commodity_code", "Commodity code is missing", is_blank("t_commodity_code")),
            ("invalid_date", "Arrival date is missing or not dd/MM/yyyy", F.col("d_date").isNull()),
            ("invalid_price", "A price is missing or not a number", ~prices_ok),
            ("price_le_zero", "A price is zero or negative",
             prices_ok & (F.least("d_min", "d_max", "d_modal") <= 0)),
            ("min_gt_max", "Min price is greater than max price", prices_ok & (F.col("d_min") > F.col("d_max"))),
            ("modal_out_of_range", "Modal price is outside the min-max range",
             prices_ok & ((F.col("d_modal") < F.col("d_min")) | (F.col("d_modal") > F.col("d_max")))),
            ("unknown_market", "Market is not in dim_market",
             ~is_blank("t_market") & F.col("dm_market_key").isNull()),
            ("unknown_commodity", "Commodity code is not in dim_commodity",
             ~is_blank("t_commodity_code") & F.col("dc_commodity_key").isNull()),
            ("future_date", "Arrival date is in the future",
             F.col("d_date").isNotNull() & (F.col("d_date") > F.current_date())),
            ("unknown_date", "Arrival date is not in dim_date",
             F.col("d_date").isNotNull() & F.col("dd_date_key").isNull()),
        ]
        rule_ids = F.filter(F.array(*[F.when(cond, F.lit(rid)) for rid, _, cond in rules]), lambda x: x.isNotNull())
        rule_reasons = F.filter(F.array(*[F.when(cond, F.lit(why)) for _, why, cond in rules]), lambda x: x.isNotNull())
        df = (
            df
            .withColumn("rule_ids", rule_ids)
            .withColumn("rule_reasons", rule_reasons)
            .withColumn("is_valid", F.size("rule_ids") == 0)
        )

        # Duplicates are looked for among the valid rows only (is_valid is part of the partition).
        latest_first = Window.partitionBy(
            "is_valid", "t_market", "d_code", "t_variety", "t_grade", "d_date"
        ).orderBy(F.col("_ingested_at").desc(), F.col("_row_no").desc())
        df = df.withColumn("is_duplicate", F.col("is_valid") & (F.row_number().over(latest_first) > 1))
        df = (
            df
            .withColumn(
                "reject_rule",
                F.when(F.col("is_duplicate"), F.lit("duplicate")).otherwise(F.concat_ws(",", "rule_ids")),
            )
            .withColumn(
                "reject_reason",
                F.when(
                    F.col("is_duplicate"),
                    F.lit("Same natural key as another row of the run; the latest row was kept"),
                ).otherwise(F.concat_ws("; ", "rule_reasons")),
            )
        )

        clean_df = df.where(F.col("reject_rule") == "").select(
            F.col("t_state").alias("state"),
            F.col("t_district").alias("district"),
            F.col("dm_district_name").alias("district_std"),
            F.col("t_market").alias("market"),
            F.col("d_code").alias("commodity_code"),
            F.col("dc_commodity_name").alias("commodity"),
            F.col("t_variety").alias("variety"),
            F.col("t_grade").alias("grade"),
            F.col("d_date").alias("arrival_date"),
            F.col("d_min").alias("min_price"),
            F.col("d_max").alias("max_price"),
            F.col("d_modal").alias("modal_price"),
            F.lit(run_id).alias("run_id"),
            F.col("_ingested_at").alias("ingested_at"),
            F.current_timestamp().alias("updated_at"),
        )
        rejected_df = df.where(F.col("reject_rule") != "").select(
            *[F.col(c) for c in source_columns],
            F.col("_source_mode").alias("source_mode"),
            F.col("_file_name").alias("file_name"),
            F.col("_run_id").alias("run_id"),
            F.col("_ingested_at").alias("ingested_at"),
            F.col("_row_no").alias("row_no"),
            F.col("reject_reason"),
            F.col("reject_rule"),
            F.current_timestamp().alias("rejected_at"),
            F.when(F.col("reject_rule") == "duplicate", F.lit("REJECTED")).otherwise(F.lit("ERROR")).alias("status"),
            F.current_timestamp().alias("updated_at"),
        )

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

        print(f"Silver load done for run {run_id}")
        return True
    except Exception as e:
        print(f"ERROR: silver load failed for run {run_id}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("Widget 'run_id' is required")

if not load__tables__mandi_prices(run_id):
    raise RuntimeError(f"Silver load failed for run {run_id} (see the error printed above)")

print("Silver load complete")
