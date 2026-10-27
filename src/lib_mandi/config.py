"""Schemas and Spark read options of the files the pipeline reads."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# Source file: every value is kept as a string, exactly as received.
SCHEMA_SOURCE = StructType([
    StructField("state", StringType(), True),
    StructField("district", StringType(), True),
    StructField("market", StringType(), True),
    StructField("commodity", StringType(), True),
    StructField("commodity_code", StringType(), True),
    StructField("variety", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("arrival_date", StringType(), True),
    StructField("min_price", StringType(), True),
    StructField("max_price", StringType(), True),
    StructField("modal_price", StringType(), True),
])

# Seed files: the columns of the silver dimension tables, without updated_at.
SCHEMA_DIM_MARKET = StructType([
    StructField("market_key", LongType(), True),
    StructField("state", StringType(), True),
    StructField("district_name", StringType(), True),
    StructField("market_name", StringType(), True),
])

SCHEMA_DIM_COMMODITY = StructType([
    StructField("commodity_key", LongType(), True),
    StructField("commodity_code", IntegerType(), True),
    StructField("commodity_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("is_perishable", BooleanType(), True),
])

SCHEMA_DIM_DATE = StructType([
    StructField("date_key", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("week_start", DateType(), True),
    StructField("month", IntegerType(), True),
    StructField("month_name", StringType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField("season", StringType(), True),
])


# Source file: fail on any row that does not fit the schema.
OPTIONS_READ_SOURCE = {
    "header": True,
    "mode": "FAILFAST",
    "quote": '"',
    "escape": '"',
}

# Seed files: the header names must match the schema, and a bad row fails the read.
OPTIONS_READ_SEED = {
    "header": True,
    "enforceSchema": False,
    "mode": "FAILFAST",
    "encoding": "UTF-8",
    "quote": '"',
    "escape": '"',
}
