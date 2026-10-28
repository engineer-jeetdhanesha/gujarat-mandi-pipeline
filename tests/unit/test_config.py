from pathlib import Path

import pytest
from pyspark.sql.types import StructType

from lib_mandi import config

SEED_DIR = Path(__file__).resolve().parents[2] / "seed"

# One entry per dimension: seed file, bronze schema, silver schema, silver rules.
DIMENSIONS = [
    ("dim_market.csv", config.SCHEMA_RAW_DIM_MARKET, config.SCHEMA_DIM_MARKET, config.RULES_DIM_MARKET),
    ("dim_commodity.csv", config.SCHEMA_RAW_DIM_COMMODITY, config.SCHEMA_DIM_COMMODITY, config.RULES_DIM_COMMODITY),
    ("dim_date.csv", config.SCHEMA_RAW_DIM_DATE, config.SCHEMA_DIM_DATE, config.RULES_DIM_DATE),
]


@pytest.mark.parametrize("seed_file, raw, silver, rules", DIMENSIONS, ids=[d[0] for d in DIMENSIONS])
def test__bronze_schema_matches_the_header_of_the_seed_file(
    seed_file: str, raw: StructType, silver: StructType, rules: dict
) -> None:
    # The seed files are read with enforceSchema=False, so the header must carry exactly these names.
    header = (SEED_DIR / seed_file).read_text().splitlines()[0]
    assert raw.names == header.split(",")


@pytest.mark.parametrize("seed_file, raw, silver, rules", DIMENSIONS, ids=[d[0] for d in DIMENSIONS])
def test__bronze_and_silver_have_the_same_columns_and_bronze_is_all_strings(
    seed_file: str, raw: StructType, silver: StructType, rules: dict
) -> None:
    # Silver casts the bronze strings, so the column names must line up.
    assert raw.names == silver.names
    assert {f.dataType.simpleString() for f in raw.fields} == {"string"}


@pytest.mark.parametrize("seed_file, raw, silver, rules", DIMENSIONS, ids=[d[0] for d in DIMENSIONS])
def test__rules_only_name_columns_of_the_silver_schema(
    seed_file: str, raw: StructType, silver: StructType, rules: dict
) -> None:
    # Every column named in the rules must exist, otherwise the check fails at run time.
    used = set(rules["not_null_columns"]) | {column for unique in rules["unique_columns"] for column in unique}
    if rules["date_column"]:
        used.add(rules["date_column"])
    assert used <= set(silver.names)


def test__date_gap_check_runs_on_a_date_column() -> None:
    types = {f.name: f.dataType.simpleString() for f in config.SCHEMA_DIM_DATE.fields}
    assert types[config.RULES_DIM_DATE["date_column"]] == "date"


def test__source_schema_has_eleven_string_columns() -> None:
    assert len(config.SCHEMA_SOURCE.names) == 11
    assert {f.dataType.simpleString() for f in config.SCHEMA_SOURCE.fields} == {"string"}
