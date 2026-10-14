# Databricks notebook source
# MAGIC %md
# MAGIC # Utility: shared helpers
# MAGIC
# MAGIC Helper functions used by the other notebooks. Run it at the top of each notebook with `%run ./utility`.

# COMMAND ----------

def get__default_catalog() -> str:
    """Return the name of the workspace's current (default) catalog.

    Returns:
        Catalog name, e.g. "mandi_dev"; empty string if it could not be read
        (the reason is printed).
    """
    try:
        return spark.sql("SELECT current_catalog()").first()[0]
    except Exception as e:
        print(f"ERROR: could not read the default catalog: {e}")
        return ""
