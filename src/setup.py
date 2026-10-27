# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: bronze, silver and gold
# MAGIC
# MAGIC Creates the `bronze` schema and the `bronze.raw_mandi_transactions` table, then the `silver` schema and the
# MAGIC `silver.dim_market`, `silver.dim_commodity` and `silver.dim_date` tables (loaded by `src/load_silver_dims`) and the
# MAGIC `silver.mandi_prices_clean` and `silver.mandi_prices_rejected` tables (loaded by `src/load_silver_transactions`) and the
# MAGIC `silver.fact_mandi_prices` table (loaded by `src/load_silver_facts`), then the `gold` schema and the
# MAGIC `gold.mart_price_weekly` table (loaded by `src/load_gold`),
# MAGIC in the workspace's default catalog (`SELECT current_catalog()`), which must already exist.
# MAGIC
# MAGIC Safe to re-run: every statement uses `IF NOT EXISTS`.
# MAGIC Note: `IF NOT EXISTS` skips an existing table even if its columns differ; it never alters it.
# MAGIC
# MAGIC Raises an error if any step fails, so the run shows as failed.

# COMMAND ----------

from lib_mandi.ddl import (
    create__schema,
    create__table__dim_commodity,
    create__table__dim_date,
    create__table__dim_market,
    create__table__fact_mandi_prices,
    create__table__mandi_prices_clean,
    create__table__mandi_prices_rejected,
    create__table__mart_price_weekly,
    create__table__raw_mandi_transactions,
)
from lib_mandi.utility import get__default_catalog

catalog = get__default_catalog(spark)
print(f"Setting up bronze, silver and gold in catalog: {catalog}")

if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

# (schema, comment, table creators)
layers = [
    ("bronze", "Raw data stored exactly as received", [create__table__raw_mandi_transactions]),
    (
        "silver",
        "Cleaned, typed and standardised data, dimensions and facts",
        [
            create__table__dim_market,
            create__table__dim_commodity,
            create__table__dim_date,
            create__table__mandi_prices_clean,
            create__table__mandi_prices_rejected,
            create__table__fact_mandi_prices,
        ],
    ),
    ("gold", "Data marts for the dashboard", [create__table__mart_price_weekly]),
]

for schema, comment, creators in layers:
    if not create__schema(spark, catalog, schema, comment):
        raise RuntimeError(f"Could not create schema {catalog}.{schema} (see the error printed above)")
    print(f"Schema ready: {catalog}.{schema}")

    for create__table in creators:
        table = create__table.__name__.replace("create__table__", "")
        if not create__table(spark, catalog, schema):
            raise RuntimeError(f"Could not create table {catalog}.{schema}.{table} (see the error printed above)")
        print(f"Table ready: {catalog}.{schema}.{table}")

print("Setup complete")
