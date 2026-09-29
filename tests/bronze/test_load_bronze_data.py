# Databricks notebook source
# MAGIC %md
# MAGIC # Test: load__bronze__mandi_raw__with_api
# MAGIC
# MAGIC Tests the bronze load function from `src/bronze/load_bronze_data` with the API mocked:
# MAGIC `fetch__api__mandi_records` is patched to return 10 sample rows
# MAGIC (from `data/mandi_gujarat_2026_backfill.csv`, 01/01/2026). No API call is made.
# MAGIC
# MAGIC Rows are written to `catalog`.bronze.mandi_raw with a unique test `_run_id`
# MAGIC and deleted after the tests. Requires the table to exist (`setup_bronze`).

# COMMAND ----------

# MAGIC %pip install pytest pytest-mock ipytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import uuid
from datetime import date

dbutils.widgets.text("catalog", "gujarat_mandi_pipeline_dev_ws", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

TABLE_NAME = f"{catalog}.bronze.mandi_raw"
TEST_RUN_ID = f"pytest-{uuid.uuid4()}"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## pytest setup

# COMMAND ----------

import base64
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
