# Databricks notebook source
# MAGIC %md
# MAGIC # Test: bronze mandi_raw loads (API and file)
# MAGIC
# MAGIC Tests the bronze load functions from `src/bronze/load_bronze_data`.
# MAGIC
# MAGIC - **API** (`load__bronze__mandi_raw__with_api`): `fetch__api__mandi_records` is patched to
# MAGIC   return 10 sample rows (from `data/mandi_gujarat_2026_backfill.csv`, 01/01/2026). No API call is made.
# MAGIC - **File** (`load__bronze__mandi_raw__with_file`): the same 10 rows are written as CSV files to a
# MAGIC   temporary folder inside the `bronze.raw_files` volume, and `FILE_VOLUME_DIR` is patched to that
# MAGIC   folder, so real files in the volume are never read.
# MAGIC
# MAGIC Rows are written to `catalog`.bronze.mandi_raw with a unique test `_run_id`
# MAGIC and deleted after the tests; the temporary folder is removed too.
# MAGIC Requires the table and the volume to exist (`setup_bronze`).

# COMMAND ----------

# MAGIC %pip install pytest pytest-mock ipytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import os
import uuid
from datetime import date

dbutils.widgets.text("catalog", "gujarat_mandi_pipeline_dev_ws", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

TABLE_NAME = f"{catalog}.bronze.mandi_raw"
TEST_RUN_ID = f"pytest-{uuid.uuid4()}"
FILE_RUN_ID = f"{TEST_RUN_ID}-file"
# Temporary folder in the bronze.raw_files volume; the loader's glob is not recursive,
# so the real mandi_*.csv files in the volume are not picked up.
FILE_TEST_DIR = f"/Volumes/{catalog}/bronze/raw_files/_pytest_{uuid.uuid4()}"
ARRIVAL_DATE = date(2026, 1, 1)
API_KEY = "dummy-key"

# Workspace path of src/bronze/load_bronze_data, from this notebook's path (tests/bronze/...).
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = notebook_path.removeprefix("/Workspace").rsplit("/tests/", 1)[0]
LOAD_BRONZE_NOTEBOOK = f"{repo_root}/src/bronze/load_bronze_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample data
# MAGIC 10 rows from `data/mandi_gujarat_2026_backfill.csv`, shaped like API records.

# COMMAND ----------

SAMPLE_COLUMNS = ["State", "District", "Market", "Commodity", "Commodity_Code", "Variety", "Grade",
                  "Arrival_Date", "Min_Price", "Max_Price", "Modal_Price"]

SAMPLE_ROWS = [
    ("Gujarat", "Vadodara(Baroda)", "Vadodara(Sayajipura) APMC", "Bitter gourd", "81", "Other", "FAQ", "01/01/2026", "550", "550", "550"),
    ("Gujarat", "Bharuch", "Bharuch APMC", "Pomegranate", "190", "Other", "Local", "01/01/2026", "2500", "3500", "3000"),
    ("Gujarat", "Anand", "Anand(Veg,Yard,Anand) APMC", "Brinjal", "35", "Brinjal", "Grade B", "01/01/2026", "1000", "1500", "1250"),
    ("Gujarat", "Banaskanth", "Deesa(Deesa Veg Yard) APMC", "Bottle gourd", "82", "Other", "FAQ", "01/01/2026", "500", "800", "650"),
    ("Gujarat", "Banaskanth", "Deesa(Deesa Veg Yard) APMC", "Peas cod", "308", "Other", "FAQ", "01/01/2026", "1600", "2200", "1900"),
    ("Gujarat", "Amreli", "Savarkundla APMC", "Groundnut", "10", "Bold", "Local", "01/01/2026", "5500", "6500", "5900"),
    ("Gujarat", "Morbi", "Morbi APMC", "Sesamum(Sesame,Gingelly,Til)", "11", "White", "FAQ", "01/01/2026", "6500", "9870", "8185"),
    ("Gujarat", "Amreli", "Dhari APMC", "Sesamum(Sesame,Gingelly,Til)", "11", "White", "Non-FAQ", "01/01/2026", "7500", "7500", "7500"),
    ("Gujarat", "Amreli", "Babra APMC", "Sesamum(Sesame,Gingelly,Til)", "11", "White", "FAQ", "01/01/2026", "7700", "10000", "8850"),
    ("Gujarat", "Rajkot", "Dhoraji APMC", "Groundnut", "10", "Hybrid", "Non-FAQ", "01/01/2026", "5480", "6505", "6230"),
]

SAMPLE_RECORDS = [dict(zip(SAMPLE_COLUMNS, row)) for row in SAMPLE_ROWS]

# File mode: 6 rows in one file and 4 in another; a third file must be ignored.
FILE_A_ROWS, FILE_B_ROWS = SAMPLE_ROWS[:6], SAMPLE_ROWS[6:]
IGNORED_ROW = ("Gujarat", "Kutch", "Bhuj APMC", "Wheat", "1", "Other", "FAQ", "01/01/2026", "1", "2", "3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## pytest setup

# COMMAND ----------

import base64
import csv
import sys

import ipytest
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

# Keep pytest from writing __pycache__ into the Git folder.
sys.dont_write_bytecode = True

ipytest.autoconfig(raise_on_error=True, addopts=["-p", "no:cacheprovider"])


def load__functions__load_bronze_data() -> dict:
    """Return the constants and functions of load_bronze_data as a namespace.

    Exports the notebook source and runs only the cells that don't use dbutils
    (constants and function definitions), so the Parameters and Run cells
    (widgets, secret, real API call, notebook.exit) are skipped.
    """
    exported = WorkspaceClient().workspace.export(LOAD_BRONZE_NOTEBOOK, format=ExportFormat.SOURCE)
    source = base64.b64decode(exported.content).decode("utf-8")

    namespace = {"spark": spark}
    for cell in source.split("# COMMAND ----------"):
        if "dbutils." not in cell:
            exec(cell, namespace)
    return namespace

# COMMAND ----------

@pytest.fixture(scope="module")
def load_ns():
    """Namespace holding load__bronze__mandi_raw__with_api and its constants."""
    return load__functions__load_bronze_data()


@pytest.fixture(scope="module")
def mock_fetch(load_ns, module_mocker):
    """Patch fetch__api__mandi_records to return the sample records (no API call)."""
    mock = module_mocker.MagicMock(return_value=SAMPLE_RECORDS)
    module_mocker.patch.dict(load_ns, {"fetch__api__mandi_records": mock})
    return mock


@pytest.fixture(scope="module", autouse=True)
def loaded(load_ns, mock_fetch):
    """Run the load once with the API mocked, then delete the test rows."""
    row_count = load_ns["load__bronze__mandi_raw__with_api"](catalog, ARRIVAL_DATE, API_KEY, TEST_RUN_ID)

    yield row_count

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE _run_id LIKE '{TEST_RUN_ID}%'")


@pytest.fixture(scope="module")
def written_rows():
    """Rows written by the test run."""
    return spark.table(TABLE_NAME).where(f"_run_id = '{TEST_RUN_ID}'").collect()


def write_csv(directory: str, name: str, rows: list) -> None:
    """Write a header + rows CSV (quoting as needed) into a volume folder."""
    os.makedirs(directory, exist_ok=True)
    with open(f"{directory}/{name}", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(SAMPLE_COLUMNS)
        writer.writerows(rows)


@pytest.fixture(scope="module")
def file_loaded(load_ns, module_mocker):
    """Write test CSVs to a temp volume folder, run the file load once, then clean up."""
    write_csv(FILE_TEST_DIR, "mandi_a.csv", FILE_A_ROWS)
    write_csv(FILE_TEST_DIR, "mandi_b.csv", FILE_B_ROWS)
    write_csv(FILE_TEST_DIR, "other.csv", [IGNORED_ROW])  # does not match mandi_*.csv
    module_mocker.patch.dict(load_ns, {"FILE_VOLUME_DIR": FILE_TEST_DIR})

    row_count = load_ns["load__bronze__mandi_raw__with_file"](catalog, FILE_RUN_ID)

    yield row_count

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE _run_id = '{FILE_RUN_ID}'")
    dbutils.fs.rm(FILE_TEST_DIR, True)


@pytest.fixture(scope="module")
def file_written_rows(file_loaded):
    """Rows written by the file-mode test run."""
    return spark.table(TABLE_NAME).where(f"_run_id = '{FILE_RUN_ID}'").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests

# COMMAND ----------

def test__load__returns_row_count(loaded):
    """The function returns the number of records fetched."""
    assert loaded == len(SAMPLE_RECORDS)


def test__load__calls_fetch_with_key_and_date(mock_fetch):
    """The API fetch is called once with the API key and arrival date."""
    mock_fetch.assert_called_once_with(API_KEY, ARRIVAL_DATE)


def test__table__mandi_raw_row_count(written_rows):
    """All sample rows are written to bronze.mandi_raw."""
    assert len(written_rows) == len(SAMPLE_RECORDS)


def test__table__mandi_raw_values_as_received(written_rows):
    """Source values are stored unchanged, as strings."""
    actual = sorted(tuple(row[c] for c in SAMPLE_COLUMNS) for row in written_rows)
    assert actual == sorted(SAMPLE_ROWS)


def test__table__mandi_raw_metadata(written_rows):
    """Metadata columns: _source = 'api', _source_file NULL, _ingested_at set."""
    assert {row["_source"] for row in written_rows} == {"api"}
    assert all(row["_source_file"] is None for row in written_rows)
    assert all(row["_ingested_at"] is not None for row in written_rows)


def test__load__no_records_writes_nothing(load_ns, mocker):
    """When the API returns no records, the function returns 0 and writes no rows."""
    empty_run_id = f"{TEST_RUN_ID}-empty"
    mocker.patch.dict(load_ns, {"fetch__api__mandi_records": mocker.MagicMock(return_value=[])})

    row_count = load_ns["load__bronze__mandi_raw__with_api"](catalog, ARRIVAL_DATE, API_KEY, empty_run_id)

    assert row_count == 0
    assert spark.table(TABLE_NAME).where(f"_run_id = '{empty_run_id}'").count() == 0


def test__file_load__returns_row_count(file_loaded):
    """The function returns the rows of the mandi_*.csv files; other.csv is ignored."""
    assert file_loaded == len(SAMPLE_ROWS)


def test__file_table__mandi_raw_row_count(file_written_rows):
    """All rows of both mandi_*.csv files are written to bronze.mandi_raw."""
    assert len(file_written_rows) == len(SAMPLE_ROWS)


def test__file_table__mandi_raw_values_as_received(file_written_rows):
    """Source values are stored unchanged, as strings (comma in a field is parsed correctly)."""
    actual = sorted(tuple(row[c] for c in SAMPLE_COLUMNS) for row in file_written_rows)
    assert actual == sorted(SAMPLE_ROWS)
    assert IGNORED_ROW not in actual


def test__file_table__mandi_raw_metadata(file_written_rows):
    """Metadata columns: _source = 'file', _ingested_at set, _run_id = the test run id."""
    assert {row["_source"] for row in file_written_rows} == {"file"}
    assert all(row["_ingested_at"] is not None for row in file_written_rows)
    assert {row["_run_id"] for row in file_written_rows} == {FILE_RUN_ID}


def test__file_table__source_file_is_per_row_file(file_written_rows):
    """_source_file holds the file each row came from (compared by file name)."""
    file_by_row = {
        tuple(row[c] for c in SAMPLE_COLUMNS): os.path.basename(row["_source_file"])
        for row in file_written_rows
    }
    assert {file_by_row[row] for row in FILE_A_ROWS} == {"mandi_a.csv"}
    assert {file_by_row[row] for row in FILE_B_ROWS} == {"mandi_b.csv"}


def test__file_load__header_only_file_writes_nothing(load_ns, mocker):
    """A mandi_*.csv with only a header returns 0 and writes no rows."""
    empty_dir = f"{FILE_TEST_DIR}-empty"
    empty_run_id = f"{FILE_RUN_ID}-empty"
    write_csv(empty_dir, "mandi_empty.csv", [])
    mocker.patch.dict(load_ns, {"FILE_VOLUME_DIR": empty_dir})
    try:
        row_count = load_ns["load__bronze__mandi_raw__with_file"](catalog, empty_run_id)
    finally:
        dbutils.fs.rm(empty_dir, True)

    assert row_count == 0
    assert spark.table(TABLE_NAME).where(f"_run_id = '{empty_run_id}'").count() == 0


def test__file_load__no_matching_file_raises(load_ns, mocker):
    """When no mandi_*.csv exists in the folder, the function fails and writes nothing."""
    no_match_dir = f"{FILE_TEST_DIR}-nomatch"
    no_match_run_id = f"{FILE_RUN_ID}-nomatch"
    write_csv(no_match_dir, "other.csv", [IGNORED_ROW])
    mocker.patch.dict(load_ns, {"FILE_VOLUME_DIR": no_match_dir})
    try:
        with pytest.raises(Exception):
            load_ns["load__bronze__mandi_raw__with_file"](catalog, no_match_run_id)
    finally:
        dbutils.fs.rm(no_match_dir, True)

    assert spark.table(TABLE_NAME).where(f"_run_id = '{no_match_run_id}'").count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
