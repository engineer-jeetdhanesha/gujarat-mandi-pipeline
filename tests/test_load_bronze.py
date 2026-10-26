# Databricks notebook source
# MAGIC %md
# MAGIC # Test: bronze load
# MAGIC
# MAGIC Runs `src/load_bronze` as a child notebook on small dummy CSV files and checks what lands in
# MAGIC `bronze.raw_mandi_transactions`: metadata columns, row counts, raw values, and the errors
# MAGIC for wrong-format files and missing parameters.
# MAGIC
# MAGIC Tests are plain pytest functions run inside this notebook with `ipytest`.
# MAGIC The notebook fails if any test fails, so it can be used as a job task.
# MAGIC
# MAGIC Needs the `landing_zone` volume in the default catalog's `bronze` schema for the dummy files.
# MAGIC
# MAGIC Cleanup: after the tests, the rows loaded here (run ids starting with `pytest-`) and the dummy files are removed.
# MAGIC The table and the `bronze` schema are dropped only if this run created them.

# COMMAND ----------

# MAGIC %pip install pytest ipytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import uuid

# (name, type) of the columns added by bronze, from docs/table-structure.md.
METADATA_COLUMNS = [
    ("_ingested_at", "timestamp"),
    ("_source_mode", "string"),
    ("_file_name", "string"),
    ("_run_id", "string"),
    ("_row_no", "bigint"),
]

# Source columns in file order.
SOURCE_COLUMNS = [
    "state",
    "district",
    "market",
    "commodity",
    "commodity_code",
    "variety",
    "grade",
    "arrival_date",
    "min_price",
    "max_price",
    "modal_price",
]

# Header as it appears in the real files.
FILE_HEADER = [
    "State", "District", "Market", "Commodity", "Commodity_Code", "Variety",
    "Grade", "Arrival_Date", "Min_Price", "Max_Price", "Modal_Price",
]

RUN_PREFIX = f"pytest-{uuid.uuid4().hex[:8]}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## State before setup
# MAGIC
# MAGIC Recorded before `src/setup` runs, so cleanup only drops what this run creates.

# COMMAND ----------

test_catalog = spark.catalog.currentCatalog()
TABLE_NAME = f"{test_catalog}.bronze.raw_mandi_transactions"
TEST_FOLDER = f"/Volumes/{test_catalog}/bronze/landing_zone/_test_{RUN_PREFIX}"

schema_existed = len(spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE 'bronze'").collect()) == 1
table_existed = spark.catalog.tableExists(TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run setup

# COMMAND ----------

# MAGIC %run ../src/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## pytest setup

# COMMAND ----------

import sys

import ipytest
import pytest

# Keep pytest from writing __pycache__ into the Git folder.
sys.dont_write_bytecode = True

ipytest.autoconfig(raise_on_error=True, addopts=["-p", "no:cacheprovider"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def build__rows(n: int) -> list[list[str]]:
    """Build n dummy rows of the 11 source columns, all different.

    Args:
        n: Number of rows.

    Returns:
        List of rows; each row is a list of 11 strings.
    """
    return [
        [
            "Gujarat",
            f"District {i}",
            f"Market {i} APMC",
            f"Commodity {i}",
            str(100 + i),
            "Other",
            "FAQ",
            "01/01/2026",
            str(1000 + i),
            str(2000 + i),
            str(1500 + i),
        ]
        for i in range(1, n + 1)
    ]

# COMMAND ----------

def write__csv(file_name: str, rows: list[list[str]], header: list[str] | None = None) -> str:
    """Write a CSV file into the test folder.

    Args:
        file_name: File name, e.g. "dummy.csv".
        rows: Rows to write (any length each, so wrong-format files can be built).
        header: Header row; defaults to the real file header.

    Returns:
        Full path of the file.
    """
    import csv
    import os

    os.makedirs(TEST_FOLDER, exist_ok=True)
    path = f"{TEST_FOLDER}/{file_name}"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(FILE_HEADER if header is None else header)
        writer.writerows(rows)
    return path

# COMMAND ----------

def run__bronze(file_path: str, run_id: str) -> str:
    """Run src/load_bronze as a child notebook; raises if the notebook itself fails (blank widgets, no catalog).

    Args:
        file_path: Value for the file_path widget.
        run_id: Value for the run_id widget.

    Returns:
        The notebook's exit value (empty, bronze does not call exit).
    """
    return dbutils.notebook.run("../src/load_bronze", 600, {"file_path": file_path, "run_id": run_id})

# COMMAND ----------

def count__rows(where: str) -> int:
    """Count bronze rows matching a SQL condition.

    Args:
        where: SQL condition, e.g. "_run_id = 'abc'".

    Returns:
        Number of matching rows.
    """
    return spark.table(TABLE_NAME).where(where).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

@pytest.fixture(scope="module", autouse=True)
def clean_up() -> "Generator[None, None, None]":
    """After the tests, delete the test rows and files; drop the table/schema only if this run created them.

    No CASCADE: dropping the schema fails instead of removing anything else in it.
    """
    yield

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE _run_id LIKE '{RUN_PREFIX}%'")
    dbutils.fs.rm(TEST_FOLDER, True)

    if not table_existed:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    if not schema_existed:
        spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: metadata columns

# COMMAND ----------

def test__metadata_columns() -> None:
    """Metadata columns are filled for every row, with the right values and types."""
    run_id = f"{RUN_PREFIX}-metadata"
    file_name = "metadata.csv"
    n = 5
    run__bronze(write__csv(file_name, build__rows(n)), run_id)

    df = spark.table(TABLE_NAME).where(f"_run_id = '{run_id}'")
    assert df.count() == n

    # Types come from the table definition.
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    for name, expected_type in METADATA_COLUMNS:
        assert types[name] == expected_type, name

    # No nulls in any metadata column.
    for name, _ in METADATA_COLUMNS:
        assert df.where(f"{name} IS NULL").count() == 0, name

    row = df.select("_source_mode", "_file_name", "_run_id").distinct().collect()
    assert len(row) == 1
    assert (row[0]["_source_mode"], row[0]["_file_name"], row[0]["_run_id"]) == ("FILE", file_name, run_id)

    # One load time for the whole file; row numbers 1..n with no gaps.
    assert df.select("_ingested_at").distinct().count() == 1
    assert sorted(r["_row_no"] for r in df.select("_row_no").collect()) == list(range(1, n + 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: row count and values

# COMMAND ----------

def test__row_count_and_values_match_dummy_data() -> None:
    """Loaded row count and source values equal the dummy data, in file order."""
    run_id = f"{RUN_PREFIX}-count"
    rows = build__rows(7)
    run__bronze(write__csv("count.csv", rows), run_id)

    assert count__rows(f"_run_id = '{run_id}'") == len(rows)

    loaded = (
        spark.table(TABLE_NAME)
        .where(f"_run_id = '{run_id}'")
        .orderBy("_row_no")
        .select(*SOURCE_COLUMNS)
        .collect()
    )
    assert [list(r) for r in loaded] == rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: raw values are not cleaned

# COMMAND ----------

def test__raw_values_not_cleaned() -> None:
    """Quotes, commas, spaces and decimals come through exactly as received, as strings."""
    run_id = f"{RUN_PREFIX}-raw"
    row = [
        "Gujarat", " Banaskanth ", "Anand(Veg,Yard,Anand) APMC", "Brinjal", "35",
        "Other", "FAQ", "01/01/2026", "1250.50", "1500", "1375.25",
    ]
    run__bronze(write__csv("raw.csv", [row]), run_id)

    loaded = spark.table(TABLE_NAME).where(f"_run_id = '{run_id}'").select(*SOURCE_COLUMNS).collect()
    assert len(loaded) == 1
    assert list(loaded[0]) == row

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: wrong format file

# COMMAND ----------

@pytest.mark.parametrize("bad_row", [
    ["Gujarat", "Anand", "Anand APMC"],                       # too few columns
    build__rows(1)[0] + ["extra", "columns", "here"],         # too many columns
], ids=["too_few_columns", "too_many_columns"])
def test__wrong_format_file_fails(bad_row: list[str]) -> None:
    """A file whose rows do not match the 11-column schema loads nothing."""
    run_id = f"{RUN_PREFIX}-badformat-{len(bad_row)}"
    file_name = f"badformat_{len(bad_row)}.csv"
    path = write__csv(file_name, [bad_row])

    run__bronze(path, run_id)

    assert count__rows(f"_run_id = '{run_id}'") == 0

# COMMAND ----------

def test__header_only_file_fails() -> None:
    """A file with a header but no data rows loads nothing."""
    run_id = f"{RUN_PREFIX}-headeronly"
    path = write__csv("headeronly.csv", [])

    run__bronze(path, run_id)

    assert count__rows(f"_run_id = '{run_id}'") == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: missing parameters

# COMMAND ----------

def test__blank_file_path_fails() -> None:
    """A blank file_path raises."""
    with pytest.raises(Exception):
        run__bronze("", f"{RUN_PREFIX}-nopath")

    assert count__rows(f"_run_id = '{RUN_PREFIX}-nopath'") == 0

# COMMAND ----------

def test__blank_run_id_fails() -> None:
    """A blank run_id raises and loads nothing from the file."""
    path = write__csv("norunid.csv", build__rows(2))

    with pytest.raises(Exception):
        run__bronze(path, "")

    assert count__rows("_file_name = 'norunid.csv'") == 0

# COMMAND ----------

def test__missing_file_fails() -> None:
    """A file_path that does not exist loads nothing."""
    run_id = f"{RUN_PREFIX}-missingfile"

    run__bronze(f"{TEST_FOLDER}/does_not_exist.csv", run_id)

    assert count__rows(f"_run_id = '{run_id}'") == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
