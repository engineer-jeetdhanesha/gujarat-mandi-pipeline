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

from lib_mandi.config import SCHEMA_SOURCE
from lib_mandi.extract import extract__silver_transactions
from lib_mandi.load import load__silver_transactions
from lib_mandi.transformations import transform__silver_transactions
from lib_mandi.utility import get__default_catalog

run_id = dbutils.widgets.get("run_id").strip()

if not run_id:
    raise ValueError("Widget 'run_id' is required")

catalog = get__default_catalog(spark)
if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

data = extract__silver_transactions(
    spark,
    run_id,
    f"{catalog}.bronze.raw_mandi_transactions",
    f"{catalog}.silver.dim_market",
    f"{catalog}.silver.dim_commodity",
    f"{catalog}.silver.dim_date",
)
result = None
if data is not None:
    result = transform__silver_transactions(
        data["bronze"], data["dim_market"], data["dim_commodity"], data["dim_date"], run_id, SCHEMA_SOURCE
    )
if result is not None:
    load__silver_transactions(
        spark, result[0], result[1], f"{catalog}.silver.mandi_prices_clean", f"{catalog}.silver.mandi_prices_rejected"
    )
