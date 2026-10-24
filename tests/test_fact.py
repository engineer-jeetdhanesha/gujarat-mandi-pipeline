# Databricks notebook source
# MAGIC %md
# MAGIC # Test: silver fact load
# MAGIC
# MAGIC Runs `src/setup` and `src/seed_dims`, then runs `src/fact` as a child notebook on small dummy rows written to
# MAGIC `silver.mandi_prices_clean`, and checks what lands in `silver.fact_mandi_prices`: the surrogate keys and prices,
# MAGIC that only the rows of the given run are loaded, reruns and updates, and the errors for a missing dimension key
# MAGIC and for a blank or unknown `run_id`.
# MAGIC
# MAGIC Tests are plain pytest functions run inside this notebook with `ipytest`.
# MAGIC The notebook fails if any test fails, so it can be used as a job task.
# MAGIC
# MAGIC Dummy rows use run ids and varieties starting with `pytest-`, so they never match real data.
# MAGIC
# MAGIC Cleanup: after the tests, the dummy rows are deleted from clean and the fact table.
# MAGIC The tables and schemas that this run created are dropped; existing ones (and their data) are left as they were.

# COMMAND ----------

# MAGIC %pip install pytest ipytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import uuid

RUN_PREFIX = f"pytest-{uuid.uuid4().hex[:8]}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## State before setup
# MAGIC
# MAGIC Recorded before `src/setup` runs, so cleanup only drops what this run creates.

# COMMAND ----------

test_catalog = spark.catalog.currentCatalog()
CLEAN = f"{test_catalog}.silver.mandi_prices_clean"
FACT = f"{test_catalog}.silver.fact_mandi_prices"

# Tables and schemas that setup creates, tables first so they are dropped before their schema.
SETUP_TABLES = [
    FACT,
    CLEAN,
    f"{test_catalog}.silver.mandi_prices_rejected",
    f"{test_catalog}.silver.dim_market",
    f"{test_catalog}.silver.dim_commodity",
    f"{test_catalog}.silver.dim_date",
    f"{test_catalog}.bronze.raw_mandi_transactions",
]
SETUP_SCHEMAS = [f"{test_catalog}.silver", f"{test_catalog}.bronze"]

tables_existed = {name: spark.catalog.tableExists(name) for name in SETUP_TABLES}
schemas_existed = {
    name: len(spark.sql(f"SHOW SCHEMAS IN {test_catalog} LIKE '{name.split('.')[1]}'").collect()) == 1
    for name in SETUP_SCHEMAS
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run setup and seed

# COMMAND ----------

# MAGIC %run ../src/setup

# COMMAND ----------

# MAGIC %run ../src/seed_dims

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
# MAGIC ## Known dimension values
# MAGIC
# MAGIC The first market and commodity of the seeded dimensions are used for the dummy rows.

# COMMAND ----------

from datetime import date, datetime
from decimal import Decimal

_market = spark.table(f"{test_catalog}.silver.dim_market").orderBy("market_key").first()
_commodity = spark.table(f"{test_catalog}.silver.dim_commodity").orderBy("commodity_key").first()
MARKET_NAME = _market["market_name"]
MARKET_KEY = _market["market_key"]
DISTRICT_NAME = _market["district_name"]
COMMODITY_CODE = _commodity["commodity_code"]
COMMODITY_KEY = _commodity["commodity_key"]
COMMODITY_NAME = _commodity["commodity_name"]
DATE_KEY = 20260101

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def build__row(variety: str, **changes) -> dict:
    """Build one valid dummy clean row.

    Args:
        variety: Variety of the row; use a different one per test so grains never collide.
        **changes: Clean columns to override, e.g. market="Unknown".

    Returns:
        Dict with every column of silver.mandi_prices_clean (run_id is set by insert__clean).
    """
    row = {
        "state": "Gujarat",
        "district": DISTRICT_NAME,
        "district_std": DISTRICT_NAME,
        "market": MARKET_NAME,
        "commodity_code": COMMODITY_CODE,
        "commodity": COMMODITY_NAME,
        "variety": f"{RUN_PREFIX}-{variety}",
        "grade": "FAQ",
        "arrival_date": date(2026, 1, 1),
        "min_price": Decimal("1000.00"),
        "max_price": Decimal("2000.00"),
        "modal_price": Decimal("1500.00"),
        "ingested_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    row.update(changes)
    return row

# COMMAND ----------

def insert__clean(run_id: str, rows: list):
    """Append dummy rows to silver.mandi_prices_clean under a run id.

    Args:
        run_id: Run id of the rows.
        rows: Dicts from build__row.
    """
    schema = spark.table(CLEAN).schema
    data = [tuple({**row, "run_id": run_id}[f.name] for f in schema.fields) for row in rows]
    spark.createDataFrame(data, schema).write.mode("append").saveAsTable(CLEAN)

# COMMAND ----------

def run__fact(run_id: str) -> str:
    """Run src/fact as a child notebook; raises if the notebook fails.

    Args:
        run_id: Value for the run_id widget.

    Returns:
        The notebook's exit value (empty, fact does not call exit).
    """
    return dbutils.notebook.run("../src/fact", 600, {"run_id": run_id})

# COMMAND ----------

def get__fact(variety_suffix: str) -> list:
    """Return the fact rows of a dummy variety, as a list of Rows."""
    return spark.table(FACT).where(f"variety = '{RUN_PREFIX}-{variety_suffix}'").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

@pytest.fixture(scope="module", autouse=True)
def clean_up():
    """After the tests, delete the dummy rows; drop the tables/schemas only if this run created them.

    No CASCADE: dropping a schema fails instead of removing anything else in it.
    """
    yield

    spark.sql(f"DELETE FROM {FACT} WHERE variety LIKE '{RUN_PREFIX}%'")
    spark.sql(f"DELETE FROM {CLEAN} WHERE run_id LIKE '{RUN_PREFIX}%' OR variety LIKE '{RUN_PREFIX}%'")

    for name in SETUP_TABLES:
        if not tables_existed[name]:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
    for name in SETUP_SCHEMAS:
        if not schemas_existed[name]:
            spark.sql(f"DROP SCHEMA IF EXISTS {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: keys and prices

# COMMAND ----------

def test__fact_row_has_keys_and_prices():
    """A clean row becomes one fact row with the dimension keys, the degenerate columns and the prices."""
    run_id = f"{RUN_PREFIX}-keys"
    insert__clean(run_id, [build__row("keys", min_price=Decimal("1600.50"), modal_price=Decimal("1800.00"))])
    run__fact(run_id)

    rows = get__fact("keys")
    assert len(rows) == 1
    r = rows[0]
    assert (r["market_key"], r["commodity_key"], r["date_key"]) == (MARKET_KEY, COMMODITY_KEY, DATE_KEY)
    assert (r["variety"], r["grade"]) == (f"{RUN_PREFIX}-keys", "FAQ")
    assert (r["min_price"], r["max_price"], r["modal_price"]) == (Decimal("1600.50"), Decimal("2000.00"), Decimal("1800.00"))
    assert r["loaded_at"] is not None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: only the rows of the run

# COMMAND ----------

def test__only_rows_of_the_run_are_loaded():
    """Clean rows of another run are not loaded."""
    run_a = f"{RUN_PREFIX}-onlya"
    run_b = f"{RUN_PREFIX}-onlyb"
    insert__clean(run_a, [build__row("onlya")])
    insert__clean(run_b, [build__row("onlyb")])
    run__fact(run_a)

    assert len(get__fact("onlya")) == 1
    assert len(get__fact("onlyb")) == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: reruns and updates

# COMMAND ----------

def test__rerun_does_not_duplicate_rows():
    """Running the same run twice leaves one fact row per grain."""
    run_id = f"{RUN_PREFIX}-rerun"
    insert__clean(run_id, [build__row("rerun"), build__row("rerun", grade="Local")])
    run__fact(run_id)
    run__fact(run_id)

    assert len(get__fact("rerun")) == 2


def test__later_run_updates_the_fact_row():
    """When a later run updates a clean row, the fact row is updated and no second row is added."""
    first_run = f"{RUN_PREFIX}-update1"
    second_run = f"{RUN_PREFIX}-update2"
    insert__clean(first_run, [build__row("update", max_price=Decimal("2000.00"))])
    run__fact(first_run)

    spark.sql(f"""
        UPDATE {CLEAN}
        SET run_id = '{second_run}', max_price = 2500.00, modal_price = 1800.00
        WHERE variety = '{RUN_PREFIX}-update'
    """)
    run__fact(second_run)

    rows = get__fact("update")
    assert len(rows) == 1
    assert (rows[0]["max_price"], rows[0]["modal_price"]) == (Decimal("2500.00"), Decimal("1800.00"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: errors

# COMMAND ----------

def test__missing_dimension_key_fails():
    """A clean row whose market is not in dim_market makes the run fail and loads none of its rows."""
    run_id = f"{RUN_PREFIX}-nokey"
    insert__clean(run_id, [build__row("nokey-ok"), build__row("nokey-bad", market="Pytest Unknown Market APMC")])

    with pytest.raises(Exception):
        run__fact(run_id)

    assert len(get__fact("nokey-ok")) == 0
    assert len(get__fact("nokey-bad")) == 0


def test__blank_run_id_fails():
    """A blank run_id raises."""
    with pytest.raises(Exception):
        run__fact("")


def test__run_id_without_clean_rows_fails():
    """A run_id with no rows in clean raises."""
    with pytest.raises(Exception):
        run__fact(f"{RUN_PREFIX}-norows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
