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

from lib_mandi.extract import extract__gold_weekly_mart
from lib_mandi.load import load__gold_weekly_mart
from lib_mandi.transformations import transform__gold_weekly_mart
from lib_mandi.utility import get__default_catalog

run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("Widget 'run_id' is required")

catalog = get__default_catalog(spark)
if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

data = extract__gold_weekly_mart(
    spark,
    run_id,
    f"{catalog}.silver.mandi_prices_clean",
    f"{catalog}.silver.fact_mandi_prices",
    f"{catalog}.silver.dim_market",
    f"{catalog}.silver.dim_commodity",
    f"{catalog}.silver.dim_date",
)
df = None
if data is not None:
    df = transform__gold_weekly_mart(
        data["clean"], data["fact"], data["dim_market"], data["dim_commodity"], data["dim_date"]
    )
if df is not None:
    load__gold_weekly_mart(spark, df, f"{catalog}.gold.mart_price_weekly")
