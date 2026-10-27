"""Write each table. Each function handles its own errors: the reason is printed and False is returned."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def load__bronze(df: DataFrame, table_name: str) -> bool:
    """Append the rows to the bronze table.

    Args:
        df: Rows with the source and metadata columns (lib_mandi.transformations.transform__bronze).
        table_name: Three-level table name, e.g. "mandi_dev.bronze.raw_mandi_transactions".

    Returns:
        True if the rows were appended; False if the write failed (the reason is printed).
    """
    try:
        print(f"Appending rows to {table_name} ...")
        df.write.mode("append").saveAsTable(table_name)
        print(f"Bronze load done for {table_name}")
        return True
    except Exception as e:
        print(f"ERROR: load failed for {table_name}: {e}")
        return False


def load__silver_dims(df: DataFrame, table_name: str) -> bool:
    """Replace a dimension table with the rows (INSERT OVERWRITE).

    Args:
        df: Rows with the table's columns including updated_at (lib_mandi.transformations.transform__silver_dims).
        table_name: Three-level table name, e.g. "mandi_dev.silver.dim_market".

    Returns:
        True if the table was loaded; False if the write failed (the reason is printed).
        The table is unchanged on False.
    """
    try:
        print(f"Overwriting {table_name} ...")
        df.write.insertInto(table_name, overwrite=True)
        print(f"Loaded {table_name}")
        return True
    except Exception as e:
        print(f"ERROR: seed load failed for {table_name}: {e}")
        return False


def load__silver_transactions(
    spark: SparkSession, clean_df: DataFrame, rejected_df: DataFrame, clean_table: str, rejected_table: str
) -> bool:
    """MERGE the clean rows and insert the new rejected rows.

    Args:
        spark: Spark session.
        clean_df: Clean rows (lib_mandi.transformations.transform__silver_transactions).
        rejected_df: Rejected rows (same function).
        clean_table: Three-level name of silver.mandi_prices_clean (MERGE target).
        rejected_table: Three-level name of silver.mandi_prices_rejected (insert-only MERGE target).

    Returns:
        True if both tables were loaded; False if a MERGE failed (the reason is printed).
    """
    try:
        clean_df.createOrReplaceTempView("silver_clean_input")
        print(f"Merging clean rows into {clean_table} ...")
        spark.sql(f"""
            MERGE INTO {clean_table} AS t
            USING silver_clean_input AS s
            ON  t.market = s.market
            AND t.commodity_code = s.commodity_code
            AND t.variety <=> s.variety
            AND t.grade <=> s.grade
            AND t.arrival_date = s.arrival_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        rejected_df.createOrReplaceTempView("silver_rejected_input")
        print(f"Inserting new rejected rows into {rejected_table} ...")
        spark.sql(f"""
            MERGE INTO {rejected_table} AS t
            USING silver_rejected_input AS s
            ON  t.run_id = s.run_id
            AND t.file_name <=> s.file_name
            AND t.row_no = s.row_no
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("Silver transactions load done")
        return True
    except Exception as e:
        print(f"ERROR: silver transactions load failed: {e}")
        return False


def load__silver_facts(spark: SparkSession, df: DataFrame, fact_table: str) -> bool:
    """MERGE the rows into silver.fact_mandi_prices.

    Args:
        spark: Spark session.
        df: Fact rows (lib_mandi.transformations.transform__silver_facts).
        fact_table: Three-level name of silver.fact_mandi_prices (MERGE target).

    Returns:
        True if the rows were loaded; False if the MERGE failed (the reason is printed).
    """
    try:
        df.createOrReplaceTempView("silver_fact_input")
        print(f"Merging rows into {fact_table} ...")
        spark.sql(f"""
            MERGE INTO {fact_table} AS t
            USING silver_fact_input AS s
            ON  t.market_key = s.market_key
            AND t.commodity_key = s.commodity_key
            AND t.date_key = s.date_key
            AND t.variety <=> s.variety
            AND t.grade <=> s.grade
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("Silver facts load done")
        return True
    except Exception as e:
        print(f"ERROR: silver facts load failed: {e}")
        return False


def load__gold_weekly_mart(spark: SparkSession, df: DataFrame, mart_table: str) -> bool:
    """MERGE the weekly rows into gold.mart_price_weekly.

    Args:
        spark: Spark session.
        df: Mart rows (lib_mandi.transformations.transform__gold_weekly_mart).
        mart_table: Three-level name of gold.mart_price_weekly (MERGE target).

    Returns:
        True if the rows were loaded; False if the MERGE failed (the reason is printed).
    """
    try:
        df.createOrReplaceTempView("gold_mart_input")
        print(f"Merging weekly rows into {mart_table} ...")
        spark.sql(f"""
            MERGE INTO {mart_table} AS t
            USING gold_mart_input AS s
            ON  t.district_name = s.district_name
            AND t.commodity_code = s.commodity_code
            AND t.week_start = s.week_start
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("Gold load done")
        return True
    except Exception as e:
        print(f"ERROR: gold load failed: {e}")
        return False
