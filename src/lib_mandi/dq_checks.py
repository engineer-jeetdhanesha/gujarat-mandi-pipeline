"""Data quality checks for the silver transactions load, one function per rule.

Each check takes the rows being checked and returns them with the rule appended to the
`rule_ids` and `rule_reasons` array columns when the row FAILS the rule. The checks are run in
the order below, which is the order in which failed rule ids are listed in `reject_rule`.

Columns the checks use:
- t_<column>: source value with leading and trailing whitespace removed
- d_date, d_min, d_max, d_modal, d_code: typed values, NULL when blank or not valid
- dm_market_key, dc_commodity_key, dd_date_key: dimension lookups, NULL when not found
- rule_ids, rule_reasons: array<string>, start empty

A check that needs a valid value is skipped when that value is missing or invalid,
so one bad value gives one reason.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def dq_check__missing_market(df: DataFrame) -> DataFrame:
    """Market name is blank."""
    from pyspark.sql import functions as F

    failed = F.expr("t_market IS NULL OR t_market = ''")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("missing_market"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Market is missing"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__missing_commodity_code(df: DataFrame) -> DataFrame:
    """Commodity code is blank."""
    from pyspark.sql import functions as F

    failed = F.expr("t_commodity_code IS NULL OR t_commodity_code = ''")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("missing_commodity_code"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Commodity code is missing"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__invalid_date(df: DataFrame) -> DataFrame:
    """Arrival date is blank or not dd/MM/yyyy."""
    from pyspark.sql import functions as F

    failed = F.expr("d_date IS NULL")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("invalid_date"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Arrival date is missing or not dd/MM/yyyy"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__invalid_price(df: DataFrame) -> DataFrame:
    """A price is blank or not a number."""
    from pyspark.sql import functions as F

    failed = F.expr("d_min IS NULL OR d_max IS NULL OR d_modal IS NULL")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("invalid_price"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("A price is missing or not a number"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__price_le_zero(df: DataFrame) -> DataFrame:
    """A price is zero or negative (checked only when all three prices are valid)."""
    from pyspark.sql import functions as F

    failed = F.expr("d_min IS NOT NULL AND d_max IS NOT NULL AND d_modal IS NOT NULL AND least(d_min, d_max, d_modal) <= 0")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("price_le_zero"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("A price is zero or negative"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__min_gt_max(df: DataFrame) -> DataFrame:
    """Min price is above max price (checked only when all three prices are valid)."""
    from pyspark.sql import functions as F

    failed = F.expr("d_min IS NOT NULL AND d_max IS NOT NULL AND d_modal IS NOT NULL AND d_min > d_max")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("min_gt_max"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Min price is greater than max price"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__modal_out_of_range(df: DataFrame) -> DataFrame:
    """Modal price is below min or above max (checked only when all prices are valid and min is not above max)."""
    from pyspark.sql import functions as F

    failed = F.expr("d_min IS NOT NULL AND d_max IS NOT NULL AND d_modal IS NOT NULL AND d_min <= d_max AND (d_modal < d_min OR d_modal > d_max)")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("modal_out_of_range"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Modal price is outside the min-max range"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__unknown_market(df: DataFrame) -> DataFrame:
    """Market is filled but not in silver.dim_market."""
    from pyspark.sql import functions as F

    failed = F.expr("t_market IS NOT NULL AND t_market <> '' AND dm_market_key IS NULL")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("unknown_market"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Market is not in dim_market"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__unknown_commodity(df: DataFrame) -> DataFrame:
    """Commodity code is filled but not in silver.dim_commodity."""
    from pyspark.sql import functions as F

    failed = F.expr("t_commodity_code IS NOT NULL AND t_commodity_code <> '' AND dc_commodity_key IS NULL")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("unknown_commodity"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Commodity code is not in dim_commodity"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__future_date(df: DataFrame) -> DataFrame:
    """Arrival date is after today."""
    from pyspark.sql import functions as F

    failed = F.expr("d_date IS NOT NULL AND d_date > current_date()")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("future_date"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Arrival date is in the future"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__unknown_date(df: DataFrame) -> DataFrame:
    """Arrival date is valid but not in silver.dim_date."""
    from pyspark.sql import functions as F

    failed = F.expr("d_date IS NOT NULL AND dd_date_key IS NULL")
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("unknown_date"))).otherwise(F.col("rule_ids")))
        .withColumn("rule_reasons", F.when(failed, F.array_append("rule_reasons", F.lit("Arrival date is not in dim_date"))).otherwise(F.col("rule_reasons")))
    )


def dq_check__duplicate(df: DataFrame) -> DataFrame:
    """Row has the same natural key as a winning row.

    Only rows that passed every other check are compared (so run this last). Among rows with the
    same natural key (market, commodity code, variety, grade, arrival date) the latest _ingested_at,
    then the highest _row_no, wins; the others are marked as duplicates.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    is_valid = F.size("rule_ids") == 0
    latest_first = Window.partitionBy(
        is_valid, "t_market", "d_code", "t_variety", "t_grade", "d_date"
    ).orderBy(F.col("_ingested_at").desc(), F.col("_row_no").desc())
    failed = is_valid & (F.row_number().over(latest_first) > 1)
    return (
        df
        .withColumn("rule_ids", F.when(failed, F.array_append("rule_ids", F.lit("duplicate"))).otherwise(F.col("rule_ids")))
        .withColumn(
            "rule_reasons",
            F.when(
                failed,
                F.array_append("rule_reasons", F.lit("Same natural key as another row of the run; the latest row was kept")),
            ).otherwise(F.col("rule_reasons")),
        )
    )
