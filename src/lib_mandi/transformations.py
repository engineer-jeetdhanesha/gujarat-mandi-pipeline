"""DataFrame -> DataFrame transformations of the pipeline, grouped by layer.

Each function is self-contained: it imports what it needs and takes everything else (DataFrames,
schemas, rules) as parameters. No Databricks globals, so everything runs in CI with a local Spark session.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------


def add__ingest_metadata(df: DataFrame, run_id: str, file_name: str) -> DataFrame:
    """Add _ingested_at, _source_mode, _file_name, _run_id and _row_no (1-based, in file order).

    Keeps file order: monotonically_increasing_id follows the read order of a single file.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    row_window = Window.orderBy(F.monotonically_increasing_id())
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_mode", F.lit("FILE"))
        .withColumn("_file_name", F.lit(file_name))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_row_no", F.row_number().over(row_window).cast("bigint"))
    )


# ---------------------------------------------------------------------------
# Silver: clean and rejected
# ---------------------------------------------------------------------------


def split__clean_rejected(
    bronze: DataFrame,
    dim_market: DataFrame,
    dim_commodity: DataFrame,
    dim_date: DataFrame,
    run_id: str,
    schema: StructType,
    rules: list[tuple[str, str, str]],
) -> tuple[DataFrame, DataFrame]:
    """Check bronze rows against the data quality rules and split them into clean and rejected.

    Args:
        bronze: Bronze rows of the run (source columns plus the _ metadata columns).
        dim_market: silver.dim_market.
        dim_commodity: silver.dim_commodity.
        dim_date: silver.dim_date.
        run_id: Id of the run, written to the run_id column of the clean rows.
        schema: Schema of the source columns (lib_mandi.schemas.SCHEMA_SOURCE).
        rules: (rule id, reason, SQL condition) tuples (lib_mandi.dq_rules.DQ_RULES).

    Rows that pass every rule (and are not duplicates) go to clean, in the columns of
    silver.mandi_prices_clean. The others go to rejected, in the columns of
    silver.mandi_prices_rejected, with the raw values, the rule ids (reject_rule) and the
    reasons (reject_reason). Among valid rows with the same natural key, the latest
    _ingested_at, then the highest _row_no, is kept; the others are rejected as "duplicate".
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    df = bronze
    for c in schema.names:
        df = df.withColumn(f"t_{c}", F.regexp_replace(F.col(c), r"^\s+|\s+$", ""))
    df = (
        df
        .withColumn(
            "d_date",
            F.when(
                F.col("t_arrival_date").rlike(r"^\d{2}/\d{2}/\d{4}$"),
                F.expr("try_to_timestamp(t_arrival_date, 'dd/MM/yyyy')").cast("date"),
            ),
        )
        .withColumn("d_min", F.expr("try_cast(t_min_price AS DECIMAL(10,2))"))
        .withColumn("d_max", F.expr("try_cast(t_max_price AS DECIMAL(10,2))"))
        .withColumn("d_modal", F.expr("try_cast(t_modal_price AS DECIMAL(10,2))"))
        .withColumn("d_code", F.expr("try_cast(t_commodity_code AS INT)"))
        .join(
            F.broadcast(dim_market.select(
                F.col("market_key").alias("dm_market_key"),
                F.col("market_name").alias("dm_market_name"),
                F.col("district_name").alias("dm_district_name"),
            )),
            F.col("t_market") == F.col("dm_market_name"),
            "left",
        )
        .join(
            F.broadcast(dim_commodity.select(
                F.col("commodity_key").alias("dc_commodity_key"),
                F.col("commodity_code").alias("dc_commodity_code"),
                F.col("commodity_name").alias("dc_commodity_name"),
            )),
            F.col("d_code") == F.col("dc_commodity_code"),
            "left",
        )
        .join(
            F.broadcast(dim_date.select(
                F.col("date_key").alias("dd_date_key"),
                F.col("date").alias("dd_date"),
            )),
            F.col("d_date") == F.col("dd_date"),
            "left",
        )
    )

    rule_ids = F.array(*[F.when(F.expr(cond), F.lit(rule_id)) for rule_id, _, cond in rules])
    rule_reasons = F.array(*[F.when(F.expr(cond), F.lit(reason)) for _, reason, cond in rules])
    df = (
        df
        .withColumn("rule_ids", F.filter(rule_ids, lambda x: x.isNotNull()))
        .withColumn("rule_reasons", F.filter(rule_reasons, lambda x: x.isNotNull()))
        .withColumn("is_valid", F.size("rule_ids") == 0)
    )

    # Duplicates are looked for among the valid rows only (is_valid is part of the partition).
    latest_first = Window.partitionBy(
        "is_valid", "t_market", "d_code", "t_variety", "t_grade", "d_date"
    ).orderBy(F.col("_ingested_at").desc(), F.col("_row_no").desc())
    df = df.withColumn("is_duplicate", F.col("is_valid") & (F.row_number().over(latest_first) > 1))

    df = (
        df
        .withColumn(
            "reject_rule",
            F.when(F.col("is_duplicate"), F.lit("duplicate")).otherwise(F.concat_ws(",", "rule_ids")),
        )
        .withColumn(
            "reject_reason",
            F.when(
                F.col("is_duplicate"),
                F.lit("Same natural key as another row of the run; the latest row was kept"),
            ).otherwise(F.concat_ws("; ", "rule_reasons")),
        )
    )

    clean = df.where(F.col("reject_rule") == "").select(
        F.col("t_state").alias("state"),
        F.col("t_district").alias("district"),
        F.col("dm_district_name").alias("district_std"),
        F.col("t_market").alias("market"),
        F.col("d_code").alias("commodity_code"),
        F.col("dc_commodity_name").alias("commodity"),
        F.col("t_variety").alias("variety"),
        F.col("t_grade").alias("grade"),
        F.col("d_date").alias("arrival_date"),
        F.col("d_min").alias("min_price"),
        F.col("d_max").alias("max_price"),
        F.col("d_modal").alias("modal_price"),
        F.lit(run_id).alias("run_id"),
        F.col("_ingested_at").alias("ingested_at"),
        F.current_timestamp().alias("updated_at"),
    )
    rejected = df.where(F.col("reject_rule") != "").select(
        *[F.col(c) for c in schema.names],
        F.col("_source_mode").alias("source_mode"),
        F.col("_file_name").alias("file_name"),
        F.col("_run_id").alias("run_id"),
        F.col("_ingested_at").alias("ingested_at"),
        F.col("_row_no").alias("row_no"),
        F.col("reject_reason"),
        F.col("reject_rule"),
        F.current_timestamp().alias("rejected_at"),
        F.when(F.col("reject_rule") == "duplicate", F.lit("REJECTED")).otherwise(F.lit("ERROR")).alias("status"),
        F.current_timestamp().alias("updated_at"),
    )
    return clean, rejected


# ---------------------------------------------------------------------------
# Silver: fact
# ---------------------------------------------------------------------------


def build__fact(
    clean: DataFrame, dim_market: DataFrame, dim_commodity: DataFrame, dim_date: DataFrame
) -> DataFrame:
    """Clean rows with the surrogate keys looked up in the dimensions (left joins, so unknown keys are NULL).

    Returns the columns of silver.fact_mandi_prices.
    """
    from pyspark.sql import functions as F
    market = dim_market.select("market_key", "market_name")
    commodity = dim_commodity.select("commodity_key", "commodity_code")
    date = dim_date.select("date_key", F.col("date").alias("dim_date"))
    return (
        clean
        .join(F.broadcast(market), clean["market"] == market["market_name"], "left")
        .join(F.broadcast(commodity), clean["commodity_code"] == commodity["commodity_code"], "left")
        .join(F.broadcast(date), clean["arrival_date"] == date["dim_date"], "left")
        .select(
            F.col("market_key"),
            F.col("commodity_key"),
            F.col("date_key"),
            F.col("variety"),
            F.col("grade"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("modal_price"),
            F.current_timestamp().alias("loaded_at"),
        )
    )


def get__rows_missing_keys(fact: DataFrame) -> DataFrame:
    """Fact rows that have no market, commodity or date key."""
    from pyspark.sql import functions as F
    return fact.where(
        F.col("market_key").isNull() | F.col("commodity_key").isNull() | F.col("date_key").isNull()
    )


# ---------------------------------------------------------------------------
# Gold: weekly price mart
# ---------------------------------------------------------------------------


def build__weekly_mart(
    clean: DataFrame, fact: DataFrame, dim_market: DataFrame, dim_commodity: DataFrame, dim_date: DataFrame
) -> DataFrame:
    """Rows of gold.mart_price_weekly for the weeks the given clean rows changed.

    Recomputes the district x commodity weeks of the clean rows, plus the week before (its
    is_complete_week can change) and the week after (its prev_week_avg_price changes). Each
    week is aggregated from all its fact rows, so the result can be MERGEd on district_name,
    commodity_code and week_start.
    """
    from functools import reduce

    from pyspark.sql import functions as F
    keys = ["district_name", "commodity_code", "week_start"]

    touched = clean.select(
        F.col("district_std").alias("district_name"),
        "commodity_code",
        F.trunc("arrival_date", "week").alias("week_start"),
    ).distinct()
    rewrite = reduce(
        lambda a, b: a.union(b),
        [touched.withColumn("week_start", F.date_add("week_start", days)) for days in (-7, 0, 7)],
    ).distinct()
    # Each rewritten week also needs the week before it, for prev_week_avg_price.
    needed = rewrite.union(rewrite.withColumn("week_start", F.date_add("week_start", -7))).distinct()

    detail = (
        fact
        .join(F.broadcast(dim_market.select("market_key", "district_name")), "market_key")
        .join(
            F.broadcast(dim_commodity.select("commodity_key", "commodity_code", "commodity_name", "category")),
            "commodity_key",
        )
        .join(F.broadcast(dim_date.select("date_key", F.col("date").alias("price_date"), "week_start")), "date_key")
    )

    # First and last date loaded in the fact table, for is_complete_week.
    bounds = detail.agg(F.min("price_date").alias("first_date"), F.max("price_date").alias("last_date"))

    weekly = (
        detail
        .join(needed, keys, "left_semi")
        .groupBy(*keys, "commodity_name", "category")
        .agg(
            F.min("min_price").alias("min_price"),
            F.max("max_price").alias("max_price"),
            F.sum("modal_price").alias("sum_modal_price"),
            F.count(F.lit(1)).alias("price_records"),
            F.countDistinct("market_key").alias("markets_count"),
        )
        .withColumn("avg_modal_price", F.round(F.col("sum_modal_price") / F.col("price_records"), 2))
    )
    previous = weekly.select(
        "district_name",
        "commodity_code",
        F.date_add("week_start", 7).alias("week_start"),
        F.col("avg_modal_price").alias("prev_week_avg_price"),
    )

    return (
        weekly
        .join(previous, keys, "left")
        .join(rewrite, keys, "left_semi")
        .crossJoin(F.broadcast(bounds))
        .select(
            F.year(F.date_add("week_start", 3)).alias("year"),
            F.weekofyear("week_start").alias("week_number"),
            "week_start",
            "district_name",
            "commodity_code",
            "commodity_name",
            "category",
            F.col("min_price").cast("decimal(10,2)").alias("min_price"),
            F.col("max_price").cast("decimal(10,2)").alias("max_price"),
            F.col("avg_modal_price").cast("decimal(10,2)").alias("avg_modal_price"),
            F.col("prev_week_avg_price").cast("decimal(10,2)").alias("prev_week_avg_price"),
            F.when(
                F.col("prev_week_avg_price") > 0,
                F.round((F.col("avg_modal_price") - F.col("prev_week_avg_price")) / F.col("prev_week_avg_price") * 100, 2),
            ).cast("decimal(10,2)").alias("wow_change_pct"),
            F.col("price_records").cast("bigint").alias("price_records"),
            F.col("sum_modal_price").cast("decimal(18,2)").alias("sum_modal_price"),
            F.col("markets_count").cast("bigint").alias("markets_count"),
            (
                (F.col("week_start") >= F.col("first_date"))
                & (F.date_add("week_start", 6) <= F.col("last_date"))
            ).alias("is_complete_week"),
            F.current_timestamp().alias("updated_at"),
        )
    )
