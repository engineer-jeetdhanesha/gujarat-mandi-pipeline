# Databricks notebook source
# MAGIC %md
# MAGIC # Utility: shared helpers
# MAGIC
# MAGIC Helper functions used by the other notebooks. Run it at the top of each notebook with `%run ./utility`.

# COMMAND ----------

def get__default_catalog() -> str:
    """Return the name of the workspace's current (default) catalog.

    Returns:
        Catalog name, e.g. "mandi_dev".
    """
    return spark.sql("SELECT current_catalog()").first()[0]


def get__exit_message(success: bool, message: str = "") -> str:
    """Build the string a notebook passes to dbutils.notebook.exit.

    Args:
        success: True if the notebook succeeded.
        message: Failure detail, used only when success is False.

    Returns:
        "SUCCESS", or "FAILURE: <message>".
    """
    return "SUCCESS" if success else f"FAILURE: {message}"
