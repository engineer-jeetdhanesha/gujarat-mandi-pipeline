"""Shared pytest fixtures for the unit tests."""

from collections.abc import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, None, None]:
    """One local Spark session with Delta for the whole test run.

    Tables live in a temporary warehouse, so nothing is written into the repository.

    Args:
        tmp_path_factory: Creates the temporary warehouse folder.

    Returns:
        The shared SparkSession; stopped after the last test.
    """
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("mandi-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        # Delta is needed for MERGE INTO, which the silver and gold loads use.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Databricks writes Delta by default; plain Spark writes Parquet, which clashes with the Delta test tables.
        .config("spark.sql.sources.default", "delta")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
