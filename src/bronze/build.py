# Databricks notebook source
# MAGIC %md
# MAGIC # Build bronze
# MAGIC
# MAGIC Entry point for the bronze load. Loads `bronze.mandi_raw` for one `arrival_date`
# MAGIC from the data.gov.in API, using the functions in `load_bronze_data`.

# COMMAND ----------

# MAGIC %run ./load_bronze_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalog", "gujarat_mandi_pipeline_dev_ws", "Unity Catalog name")
dbutils.widgets.text("arrival_date", "", "Arrival date (yyyy-MM-dd)")
dbutils.widgets.text("run_id", "", "Job run ID ({{job.run_id}})")

catalog = dbutils.widgets.get("catalog")
run_id = dbutils.widgets.get("run_id").strip() or None

arrival_date_param = dbutils.widgets.get("arrival_date").strip()
try:
    arrival_date = datetime.strptime(arrival_date_param, "%Y-%m-%d").date()
except ValueError:
    raise ValueError(f"arrival_date must be yyyy-MM-dd, got '{arrival_date_param}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load: bronze.mandi_raw

# COMMAND ----------

api_key = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)

row_count = load__bronze__mandi_raw__with_api(catalog, arrival_date, api_key, run_id)
print(f"Rows loaded into {catalog}.bronze.mandi_raw for {arrival_date}: {row_count}")

dbutils.notebook.exit(str(row_count))
