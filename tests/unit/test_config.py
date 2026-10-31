"""Tests for lib_mandi.config: the raw schemas match the seed files and are all nullable strings."""

from pathlib import Path

import pytest
from pyspark.sql.types import StructType

from lib_mandi import config

SEED_DIR = Path(__file__).resolve().parents[2] / "seed"


@pytest.mark.parametrize(
    "seed_file, schema",
    [
        pytest.param("dim_market.csv", config.SCHEMA_RAW_DIM_MARKET, id="dim_market"),
        pytest.param("dim_commodity.csv", config.SCHEMA_RAW_DIM_COMMODITY, id="dim_commodity"),
        pytest.param("dim_date.csv", config.SCHEMA_RAW_DIM_DATE, id="dim_date"),
    ],
)
def test__raw_columns_match_the_seed_file_header(seed_file: str, schema: StructType) -> None:
    """Raw schema column names equal the seed CSV header.

    Args:
        seed_file: CSV name in seed/.
        schema: Raw (bronze) schema of that dimension.

    Returns:
        None.
    """
    header = (SEED_DIR / seed_file).read_text().splitlines()[0]
    assert schema.names == header.split(",")


def test__source_columns_are_the_eleven_columns_of_the_daily_file() -> None:
    """Source schema has the 11 expected columns (no sample file in the repo).

    Returns:
        None.
    """
    assert config.SCHEMA_SOURCE.names == [
        "state", "district", "market", "commodity", "commodity_code", "variety",
        "grade", "arrival_date", "min_price", "max_price", "modal_price",
    ]


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param(config.SCHEMA_RAW_DIM_MARKET, id="dim_market"),
        pytest.param(config.SCHEMA_RAW_DIM_COMMODITY, id="dim_commodity"),
        pytest.param(config.SCHEMA_RAW_DIM_DATE, id="dim_date"),
        pytest.param(config.SCHEMA_SOURCE, id="source"),
    ],
)
def test__raw_columns_are_all_strings(schema: StructType) -> None:
    """Bronze keeps values as received, so every raw column is a string.

    Args:
        schema: Raw (bronze) schema.

    Returns:
        None.
    """
    assert {f.dataType.simpleString() for f in schema.fields} == {"string"}


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param(config.SCHEMA_RAW_DIM_MARKET, id="dim_market"),
        pytest.param(config.SCHEMA_RAW_DIM_COMMODITY, id="dim_commodity"),
        pytest.param(config.SCHEMA_RAW_DIM_DATE, id="dim_date"),
        pytest.param(config.SCHEMA_SOURCE, id="source"),
    ],
)
def test__raw_columns_are_nullable(schema: StructType) -> None:
    """Bronze accepts empty values, so every raw column is nullable.

    Args:
        schema: Raw (bronze) schema.

    Returns:
        None.
    """
    assert all(f.nullable for f in schema.fields)
