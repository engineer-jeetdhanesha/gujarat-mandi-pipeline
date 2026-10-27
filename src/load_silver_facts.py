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

from lib_mandi.extract import extract__silver_facts
from lib_mandi.load import load__silver_facts
from lib_mandi.transformations import transform__silver_facts
from lib_mandi.utility import get__default_catalog

run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("Widget 'run_id' is required")

catalog = get__default_catalog(spark)
if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

data = extract__silver_facts(
    spark,
    run_id,
    f"{catalog}.silver.mandi_prices_clean",
    f"{catalog}.silver.dim_market",
    f"{catalog}.silver.dim_commodity",
    f"{catalog}.silver.dim_date",
)
df = None
if data is not None:
    df = transform__silver_facts(data["clean"], data["dim_market"], data["dim_commodity"], data["dim_date"])
if df is not None:
    load__silver_facts(spark, df, f"{catalog}.silver.fact_mandi_prices")
