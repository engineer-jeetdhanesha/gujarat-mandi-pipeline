from collections.abc import Generator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, None, None]:
    """One local Spark session with Delta for the whole test run.

    Tables are created in a temporary warehouse, so they disappear when the run ends and
    nothing is written into the repository. Each test uses its own table names.
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
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
