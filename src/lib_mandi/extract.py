"""Read the inputs of each table load. Each function handles its own errors: the reason is printed and None is returned."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


def extract__source_paths(file_path: str) -> list[str] | None:
    """List the files that match a source path or pattern, in name order.

    Works on any path the driver can see, including Unity Catalog volumes (/Volumes/...). A path without a
    wildcard matches itself if the file exists.

    Args:
        file_path: Path of one CSV file or a pattern, e.g. "/Volumes/mandi/bronze/landing_zone/mandi_*.csv".

    Returns:
        The paths of the matching files; None if no file matches or the listing failed (the reason is printed).
    """
    import glob
    import os

    try:
        paths = sorted(path for path in glob.glob(file_path) if os.path.isfile(path))
        if not paths:
            print(f"ERROR: no file matches {file_path}")
            return None
        print(f"{len(paths)} file(s) match {file_path}")
        return paths
    except Exception as e:
        print(f"ERROR: could not list {file_path}: {e}")
        return None


def extract__source_file(
    spark: SparkSession, file_path: str, schema: StructType, options: dict[str, object]
) -> DataFrame | None:
    """Read one source CSV file.

    The file needs a header row (skipped) followed by the 11 source columns in table order.

    Args:
        spark: Spark session.
        file_path: Path of the CSV file, e.g. "/Volumes/mandi/raw/mandi_2026-01-01.csv".
        schema: Schema of the file (lib_mandi.schemas.SCHEMA_SOURCE).
        options: Spark read options (lib_mandi.config.OPTIONS_READ_SOURCE).

    Returns:
        The rows of the file; None if the file has no data rows, a row does not fit the schema
        or the read failed (the reason is printed).
    """
    try:
        print(f"Reading {file_path} ...")
        df = spark.read.schema(schema).options(**options).csv(file_path)
        if df.first() is None:
            print(f"WARNING: no data rows in {file_path}; nothing loaded")
            return None
        return df
    except Exception as e:
        print(f"ERROR: could not read {file_path}: {e}")
        return None


def extract__seed_file(
    spark: SparkSession, file_path: str, schema: StructType, options: dict[str, object]
) -> DataFrame | None:
    """Read one seed CSV file, every column as a string.

    The file needs a header row whose names equal the schema's columns, in the same order.

    Args:
        spark: Spark session.
        file_path: Path of the CSV file, e.g. "/Workspace/Users/me/repo/seed/dim_market.csv".
        schema: Schema of the file with string columns (lib_mandi.config.SCHEMA_RAW_DIM_*).
        options: Spark read options (lib_mandi.config.OPTIONS_READ_SEED).

    Returns:
        The rows of the file; None if the path is not a .csv file, the file has no data rows,
        the header or a value does not fit the schema or the read failed (the reason is printed).
    """
    if not file_path.lower().endswith(".csv"):
        print(f"ERROR: {file_path} is not a .csv file")
        return None

    try:
        read_path = f"file:{file_path}" if file_path.startswith("/Workspace") else file_path
        print(f"Reading {file_path} ...")
        df = spark.read.schema(schema).options(**options).csv(read_path)
        if df.first() is None:
            print(f"ERROR: no data rows in {file_path}; nothing loaded")
            return None
        return df
    except Exception as e:
        print(f"ERROR: could not read {file_path}: {e}")
        return None


def extract__bronze_dims(spark: SparkSession, run_id: str, bronze_table: str) -> DataFrame | None:
    """Read the rows one run loaded into a bronze seed table.

    Args:
        spark: Spark session.
        run_id: Id of the run that loaded the seed file (bronze _run_id).
        bronze_table: Three-level table name, e.g. "mandi_dev.bronze.raw_dim_market".

    Returns:
        The rows of the run; None if the run has no rows in the table or the read failed
        (the reason is printed).
    """
    from pyspark.sql import functions as F

    try:
        print(f"Reading run {run_id} from {bronze_table} ...")
        df = spark.table(bronze_table).where(F.col("_run_id") == run_id)
        if df.count() == 0:
            print(f"ERROR: no rows for run {run_id} in {bronze_table}; silver table left unchanged")
            return None
        return df
    except Exception as e:
        print(f"ERROR: could not read {bronze_table}: {e}")
        return None


def extract__bronze_file_names(spark: SparkSession, run_id: str, bronze_table: str) -> list[str] | None:
    """List the source files one run loaded into bronze, by _file_name.

    Args:
        spark: Spark session.
        run_id: Id of the run that loaded the files (bronze _run_id).
        bronze_table: Three-level table name, e.g. "mandi_dev.bronze.raw_mandi_transactions".

    Returns:
        The distinct file names of the run, in name order; None if the run has no rows in the table or the read
        failed (the reason is printed).
    """
    from pyspark.sql import functions as F

    try:
        print(f"Listing the files of run {run_id} in {bronze_table} ...")
        rows = spark.table(bronze_table).where(F.col("_run_id") == run_id).select("_file_name").distinct().collect()
        if not rows:
            print(f"ERROR: no rows for run {run_id} in {bronze_table}; no file to archive")
            return None
        return sorted(row["_file_name"] for row in rows)
    except Exception as e:
        print(f"ERROR: could not read {bronze_table}: {e}")
        return None


def extract__bronze_and_dims(
    spark: SparkSession,
    run_id: str,
    bronze_table: str,
    dim_market_table: str,
    dim_commodity_table: str,
    dim_date_table: str,
) -> dict[str, DataFrame] | None:
    """Read the bronze rows of one run and the dimensions.

    Args:
        spark: Spark session.
        run_id: Id of the run whose rows in the bronze table are processed.
        bronze_table: Three-level name of bronze.raw_mandi_transactions.
        dim_market_table: Three-level name of silver.dim_market.
        dim_commodity_table: Three-level name of silver.dim_commodity.
        dim_date_table: Three-level name of silver.dim_date.

    Returns:
        {"bronze", "dim_market", "dim_commodity", "dim_date"} DataFrames; None if the run has no
        bronze rows, a dimension is empty or the read failed (the reason is printed).
    """
    from pyspark.sql import functions as F

    try:
        bronze = spark.table(bronze_table).where(F.col("_run_id") == run_id)
        if bronze.count() == 0:
            print(f"WARNING: no rows in {bronze_table} for run_id {run_id}; nothing loaded")
            return None

        dims = {
            "dim_market": spark.table(dim_market_table),
            "dim_commodity": spark.table(dim_commodity_table),
            "dim_date": spark.table(dim_date_table),
        }
        for name, dim in dims.items():
            if dim.count() == 0:
                print(f"ERROR: {name} is empty; run src/load_silver_dims first")
                return None
        return {"bronze": bronze, **dims}
    except Exception as e:
        print(f"ERROR: could not read the inputs for run {run_id}: {e}")
        return None


def extract__silver_clean_and_dims(
    spark: SparkSession,
    run_id: str,
    clean_table: str,
    dim_market_table: str,
    dim_commodity_table: str,
    dim_date_table: str,
) -> dict[str, DataFrame] | None:
    """Read the clean rows of one run and the dimensions.

    Args:
        spark: Spark session.
        run_id: Id of the run whose rows in the clean table are loaded.
        clean_table: Three-level name of silver.mandi_prices_clean.
        dim_market_table: Three-level name of silver.dim_market.
        dim_commodity_table: Three-level name of silver.dim_commodity.
        dim_date_table: Three-level name of silver.dim_date.

    Returns:
        {"clean", "dim_market", "dim_commodity", "dim_date"} DataFrames; None if the run has no
        clean rows or the read failed (the reason is printed).
    """
    from pyspark.sql import functions as F

    try:
        clean = spark.table(clean_table).where(F.col("run_id") == run_id)
        if clean.count() == 0:
            print(f"WARNING: no rows in {clean_table} for run_id {run_id}; nothing loaded")
            return None
        return {
            "clean": clean,
            "dim_market": spark.table(dim_market_table),
            "dim_commodity": spark.table(dim_commodity_table),
            "dim_date": spark.table(dim_date_table),
        }
    except Exception as e:
        print(f"ERROR: could not read the inputs for run {run_id}: {e}")
        return None


def extract__silver_clean_fact_and_dims(
    spark: SparkSession,
    run_id: str,
    clean_table: str,
    fact_table: str,
    dim_market_table: str,
    dim_commodity_table: str,
    dim_date_table: str,
) -> dict[str, DataFrame] | None:
    """Read the clean rows of one run, the fact table and the dimensions.

    Args:
        spark: Spark session.
        run_id: Id of the run whose rows in the clean table decide the weeks.
        clean_table: Three-level name of silver.mandi_prices_clean.
        fact_table: Three-level name of silver.fact_mandi_prices.
        dim_market_table: Three-level name of silver.dim_market.
        dim_commodity_table: Three-level name of silver.dim_commodity.
        dim_date_table: Three-level name of silver.dim_date.

    Returns:
        {"clean", "fact", "dim_market", "dim_commodity", "dim_date"} DataFrames; None if the run
        has no clean rows, the fact table is empty or the read failed (the reason is printed).
    """
    from pyspark.sql import functions as F

    try:
        clean = spark.table(clean_table).where(F.col("run_id") == run_id)
        if clean.count() == 0:
            print(f"WARNING: no rows in {clean_table} for run_id {run_id}; nothing loaded")
            return None

        fact = spark.table(fact_table)
        if fact.limit(1).count() == 0:
            print(f"ERROR: {fact_table} is empty; run src/load_silver_facts first")
            return None
        return {
            "clean": clean,
            "fact": fact,
            "dim_market": spark.table(dim_market_table),
            "dim_commodity": spark.table(dim_commodity_table),
            "dim_date": spark.table(dim_date_table),
        }
    except Exception as e:
        print(f"ERROR: could not read the inputs for run {run_id}: {e}")
        return None
