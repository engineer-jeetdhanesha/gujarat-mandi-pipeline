# Databricks notebook source
# MAGIC %md
# MAGIC # Build bronze
# MAGIC
# MAGIC Entry point for the bronze load. Runs each bronze load notebook in order,
# MAGIC passing the same parameters to each.
# MAGIC
# MAGIC | Step | Notebook | Loads |
# MAGIC |------|----------|-------|
# MAGIC | 1 | `load_bronze_data` | `bronze.mandi_raw` from the data.gov.in API or a CSV file (`mode`) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "gujarat_mandi_pipeline_dev_ws", "Unity Catalog name")
dbutils.widgets.text("arrival_date", "", "Arrival date (yyyy-MM-dd)")
dbutils.widgets.text("run_id", "", "Job run ID ({{job.run_id}})")
dbutils.widgets.text("mode", "api", "Load mode (api / file)")

catalog = dbutils.widgets.get("catalog")
arrival_date = dbutils.widgets.get("arrival_date")
run_id = dbutils.widgets.get("run_id")
mode = dbutils.widgets.get("mode")

# Max seconds each child notebook may run before it is failed.
NOTEBOOK_TIMEOUT_SECONDS = 1800

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load: bronze.mandi_raw

# COMMAND ----------

dbutils.notebook.run(
    "./load_bronze_data",
    NOTEBOOK_TIMEOUT_SECONDS,
    {"catalog": catalog, "arrival_date": arrival_date, "run_id": run_id, "mode": mode},
)
