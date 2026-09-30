# Databricks notebook source
# MAGIC %md
# MAGIC # Build setup
# MAGIC
# MAGIC Entry point for environment setup. Runs each layer's setup notebook in order,
# MAGIC passing the same `catalog` to each.
# MAGIC
# MAGIC | Step | Notebook | Creates |
# MAGIC |------|----------|---------|
# MAGIC | 1 | `setup_bronze` | `bronze` schema, `bronze.mandi_raw`, `bronze.raw_files` volume |
# MAGIC
# MAGIC Safe to re-run: every child notebook uses `IF NOT EXISTS`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "gujarat_mandi_pipeline_dev_ws", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

# Max seconds each child notebook may run before it is failed.
NOTEBOOK_TIMEOUT_SECONDS = 600

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: bronze

# COMMAND ----------

dbutils.notebook.run("./setup_bronze", NOTEBOOK_TIMEOUT_SECONDS, {"catalog": catalog})
