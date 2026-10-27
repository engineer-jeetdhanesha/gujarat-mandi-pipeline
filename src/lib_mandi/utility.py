"""Small helpers shared by the notebooks."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get__default_catalog(spark: SparkSession) -> str:
    """Return the name of the workspace's current (default) catalog.

    Args:
        spark: Spark session.

    Returns:
        Catalog name, e.g. "mandi_dev"; empty string if it could not be read
        (the reason is printed).
    """
    try:
        return spark.sql("SELECT current_catalog()").first()[0]
    except Exception as e:
        print(f"ERROR: could not read the default catalog: {e}")
        return ""
