# Databricks notebook source
# MAGIC %md
# MAGIC # Test: silver load
# MAGIC
# MAGIC Runs `src/setup` and `src/seed_dims`, then runs `src/silver` as a child notebook on small dummy rows written
# MAGIC to `bronze.raw_mandi_transactions`, and checks what lands in `silver.mandi_prices_clean` and
# MAGIC `silver.mandi_prices_rejected`: the clean row values, each quality rule, several rules on one row,
# MAGIC duplicates, row counts, reruns, and the errors for a blank or unknown `run_id`.
# MAGIC
# MAGIC Tests are plain pytest functions run inside this notebook with `ipytest`.
# MAGIC The notebook fails if any test fails, so it can be used as a job task.
# MAGIC
# MAGIC Dummy rows use run ids and varieties starting with `pytest-`, so they never match real data.
# MAGIC
# MAGIC Cleanup: after the tests, the dummy rows are deleted from bronze, clean and rejected.
# MAGIC The tables and schemas that this run created are dropped; existing ones (and their data) are left as they were.

# COMMAND ----------

# MAGIC %pip install pytest ipytest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

import uuid

# Source columns in bronze order.
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

RUN_PREFIX = f"pytest-{uuid.uuid4().hex[:8]}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## State before setup
# MAGIC
# MAGIC Recorded before `src/setup` runs, so cleanup only drops what this run creates.

# COMMAND ----------

test_catalog = spark.catalog.currentCatalog()
BRONZE = f"{test_catalog}.bronze.raw_mandi_transactions"
CLEAN = f"{test_catalog}.silver.mandi_prices_clean"
REJECTED = f"{test_catalog}.silver.mandi_prices_rejected"

# Tables and schemas that setup creates, tables first so they are dropped before their schema.
SETUP_TABLES = [
    CLEAN,
    REJECTED,
    f"{test_catalog}.silver.dim_market",
    f"{test_catalog}.silver.dim_commodity",
    f"{test_catalog}.silver.dim_date",
    BRONZE,
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
# MAGIC The first market and commodity of the seeded dimensions are used for valid dummy rows.

# COMMAND ----------

from datetime import date, datetime, timedelta
from decimal import Decimal

_market = spark.table(f"{test_catalog}.silver.dim_market").orderBy("market_key").first()
_commodity = spark.table(f"{test_catalog}.silver.dim_commodity").orderBy("commodity_key").first()
MARKET_NAME = _market["market_name"]
DISTRICT_NAME = _market["district_name"]
COMMODITY_CODE = _commodity["commodity_code"]
COMMODITY_NAME = _commodity["commodity_name"]

# Last day in dim_date, to build dates that are (or are not) covered by it.
LAST_DIM_DATE = spark.table(f"{test_catalog}.silver.dim_date").agg({"date": "max"}).first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def build__row(variety: str, **changes) -> dict:
    """Build one valid dummy bronze row (the 11 source columns as strings).

    Args:
        variety: Variety of the row; use a different one per test so natural keys never collide.
        **changes: Source columns to override, e.g. min_price="abc". The special keys _row_no and
            _ingested_at are passed through to insert__bronze.

    Returns:
        Dict of the source columns (plus any special keys).
    """
    row = {
        "state": "Gujarat",
        "district": DISTRICT_NAME,
        "market": MARKET_NAME,
        "commodity": COMMODITY_NAME,
        "commodity_code": str(COMMODITY_CODE),
        "variety": f"{RUN_PREFIX}-{variety}",
        "grade": "FAQ",
        "arrival_date": "01/01/2026",
        "min_price": "1000",
        "max_price": "2000",
        "modal_price": "1500",
    }
    row.update(changes)
    return row

# COMMAND ----------

def insert__bronze(run_id: str, rows: list):
    """Append dummy rows to bronze under a run id.

    Args:
        run_id: Run id of the rows.
        rows: Dicts from build__row. _row_no defaults to 1, 2, 3 ... in list order and _ingested_at
            to now.
    """
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    schema = StructType(
        [StructField(c, StringType(), True) for c in SOURCE_COLUMNS]
        + [
            StructField("_ingested_at", TimestampType(), True),
            StructField("_source_mode", StringType(), True),
            StructField("_file_name", StringType(), True),
            StructField("_run_id", StringType(), True),
            StructField("_row_no", LongType(), True),
        ]
    )
    now = datetime.now()
    data = []
    for i, row in enumerate(rows, 1):
        values = dict(row)
        ingested_at = values.pop("_ingested_at", now)
        row_no = values.pop("_row_no", i)
        data.append(tuple(values[c] for c in SOURCE_COLUMNS) + (ingested_at, "FILE", "pytest.csv", run_id, row_no))
    spark.createDataFrame(data, schema).write.mode("append").saveAsTable(BRONZE)

# COMMAND ----------

def run__silver(run_id: str) -> str:
    """Run src/silver as a child notebook; raises if the notebook fails.

    Args:
        run_id: Value for the run_id widget.

    Returns:
        The notebook's exit value (empty, silver does not call exit).
    """
    return dbutils.notebook.run("../src/silver", 600, {"run_id": run_id})

# COMMAND ----------

def get__clean(run_id: str) -> list:
    """Return the clean rows written by a run, as a list of Rows."""
    return spark.table(CLEAN).where(f"run_id = '{run_id}'").collect()


def get__rejected(run_id: str) -> list:
    """Return the rejected rows of a run, as a list of Rows ordered by row number."""
    return spark.table(REJECTED).where(f"run_id = '{run_id}'").orderBy("row_no").collect()

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

    spark.sql(f"DELETE FROM {BRONZE} WHERE _run_id LIKE '{RUN_PREFIX}%'")
    spark.sql(f"DELETE FROM {CLEAN} WHERE run_id LIKE '{RUN_PREFIX}%' OR variety LIKE '{RUN_PREFIX}%'")
    spark.sql(f"DELETE FROM {REJECTED} WHERE run_id LIKE '{RUN_PREFIX}%'")

    for name in SETUP_TABLES:
        if not tables_existed[name]:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
    for name in SETUP_SCHEMAS:
        if not schemas_existed[name]:
            spark.sql(f"DROP SCHEMA IF EXISTS {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: a valid row goes to clean

# COMMAND ----------

def test__valid_row_goes_to_clean():
    """A valid row is trimmed, typed and standardised from the dimensions, and nothing is rejected."""
    run_id = f"{RUN_PREFIX}-valid"
    row = build__row(
        "valid",
        state=" Gujarat ",
        district=" Banaskanth ",
        market=f"  {MARKET_NAME}\n",
        commodity="any spelling",
        commodity_code=f" {COMMODITY_CODE} ",
        grade=" FAQ\n",
        min_price="1600.50",
    )
    insert__bronze(run_id, [row])
    run__silver(run_id)

    clean = get__clean(run_id)
    assert len(clean) == 1
    assert len(get__rejected(run_id)) == 0

    r = clean[0]
    assert r["state"] == "Gujarat"
    assert r["district"] == "Banaskanth"
    assert r["district_std"] == DISTRICT_NAME
    assert r["market"] == MARKET_NAME
    assert r["commodity_code"] == COMMODITY_CODE
    assert r["commodity"] == COMMODITY_NAME
    assert r["variety"] == row["variety"]
    assert r["grade"] == "FAQ"
    assert r["arrival_date"] == date(2026, 1, 1)
    assert (r["min_price"], r["max_price"], r["modal_price"]) == (Decimal("1600.50"), Decimal("2000.00"), Decimal("1500.00"))
    assert r["ingested_at"] is not None and r["updated_at"] is not None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: each rule rejects
# MAGIC
# MAGIC One bad row per case; it must be the only row of its run, rejected with exactly the expected rule ids
# MAGIC (comma separated), status `ERROR`, and its raw values kept as received.

# COMMAND ----------

SOON = date.today() + timedelta(days=30)
FUTURE_RULES = "future_date" if SOON <= LAST_DIM_DATE else "future_date,unknown_date"

RULE_CASES = [
    ("missing_market", {"market": ""}, "missing_market"),
    ("missing_market_spaces", {"market": "   "}, "missing_market"),
    ("missing_commodity_code", {"commodity_code": " "}, "missing_commodity_code"),
    ("invalid_date_short_day", {"arrival_date": "1/1/2026"}, "invalid_date"),
    ("invalid_date_iso", {"arrival_date": "2026-01-01"}, "invalid_date"),
    ("invalid_date_impossible", {"arrival_date": "31/02/2026"}, "invalid_date"),
    ("invalid_date_blank", {"arrival_date": ""}, "invalid_date"),
    ("invalid_price_text", {"min_price": "abc"}, "invalid_price"),
    ("invalid_price_blank", {"modal_price": ""}, "invalid_price"),
    ("price_zero", {"min_price": "0"}, "price_le_zero"),
    ("price_negative", {"min_price": "-5"}, "price_le_zero"),
    ("min_gt_max", {"min_price": "3000", "max_price": "2000", "modal_price": "2500"}, "min_gt_max"),
    ("modal_above_max", {"modal_price": "2500"}, "modal_out_of_range"),
    ("modal_below_min", {"modal_price": "500"}, "modal_out_of_range"),
    ("unknown_market", {"market": "Pytest Unknown Market APMC"}, "unknown_market"),
    ("unknown_market_case", {"market": MARKET_NAME.upper() + "X"}, "unknown_market"),
    ("unknown_commodity_code", {"commodity_code": "999999"}, "unknown_commodity"),
    ("unknown_commodity_text", {"commodity_code": "abc"}, "unknown_commodity"),
    ("future_date", {"arrival_date": SOON.strftime("%d/%m/%Y")}, FUTURE_RULES),
    ("unknown_date", {"arrival_date": "01/01/2025"}, "unknown_date"),
]


@pytest.mark.parametrize("case, changes, expected_rule", RULE_CASES, ids=[c[0] for c in RULE_CASES])
def test__rule_rejects_row(case, changes, expected_rule):
    """A row that breaks one rule goes to rejected with that rule id and nothing reaches clean."""
    run_id = f"{RUN_PREFIX}-rule-{case}"
    row = build__row(case, **changes)
    insert__bronze(run_id, [row])
    run__silver(run_id)

    assert len(get__clean(run_id)) == 0
    rejected = get__rejected(run_id)
    assert len(rejected) == 1

    r = rejected[0]
    assert r["reject_rule"] == expected_rule
    assert r["reject_reason"]
    assert r["status"] == "ERROR"
    assert (r["source_mode"], r["file_name"], r["row_no"]) == ("FILE", "pytest.csv", 1)
    assert r["rejected_at"] is not None and r["updated_at"] is not None
    # Raw values are kept exactly as received (not trimmed or typed).
    assert {c: r[c] for c in SOURCE_COLUMNS} == {c: row[c] for c in SOURCE_COLUMNS}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: several rules on one row

# COMMAND ----------

def test__several_rules_are_listed_in_order():
    """Every failing rule is listed, comma separated in rule order, with matching reasons."""
    run_id = f"{RUN_PREFIX}-several"
    insert__bronze(run_id, [build__row("several", arrival_date="", market="Pytest Unknown Market APMC")])
    run__silver(run_id)

    rejected = get__rejected(run_id)
    assert len(rejected) == 1
    assert rejected[0]["reject_rule"] == "invalid_date,unknown_market"
    reasons = rejected[0]["reject_reason"].split("; ")
    assert len(reasons) == 2
    assert "dd/MM/yyyy" in reasons[0] and "dim_market" in reasons[1]


def test__dependent_price_rules_are_skipped():
    """A price that is not a number gives only invalid_price, not the rules that compare prices."""
    run_id = f"{RUN_PREFIX}-skipped"
    insert__bronze(run_id, [build__row("skipped", min_price="abc", max_price="0", modal_price="0")])
    run__silver(run_id)

    rejected = get__rejected(run_id)
    assert [r["reject_rule"] for r in rejected] == ["invalid_price"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: duplicates

# COMMAND ----------

def test__duplicate_keeps_highest_row_number():
    """Rows with the same natural key: the highest _row_no goes to clean, the others are rejected as duplicate."""
    run_id = f"{RUN_PREFIX}-dup"
    rows = [build__row("dup", max_price=str(2000 + i)) for i in range(3)]
    insert__bronze(run_id, rows)
    run__silver(run_id)

    clean = get__clean(run_id)
    assert len(clean) == 1
    assert clean[0]["max_price"] == Decimal("2002.00")

    rejected = get__rejected(run_id)
    assert [r["row_no"] for r in rejected] == [1, 2]
    assert {r["reject_rule"] for r in rejected} == {"duplicate"}
    assert {r["status"] for r in rejected} == {"REJECTED"}


def test__duplicate_keeps_latest_ingested_at_first():
    """The latest _ingested_at wins over a higher _row_no."""
    run_id = f"{RUN_PREFIX}-dupts"
    now = datetime.now()
    older_high_row = build__row("dupts", max_price="2111", _row_no=9, _ingested_at=now - timedelta(days=1))
    newer_low_row = build__row("dupts", max_price="2222", _row_no=1, _ingested_at=now)
    insert__bronze(run_id, [older_high_row, newer_low_row])
    run__silver(run_id)

    clean = get__clean(run_id)
    assert len(clean) == 1
    assert clean[0]["max_price"] == Decimal("2222.00")
    assert [(r["row_no"], r["reject_rule"]) for r in get__rejected(run_id)] == [(9, "duplicate")]


def test__invalid_row_does_not_win_a_duplicate():
    """Duplicates are looked for among valid rows: an invalid top row does not push a valid row out of clean."""
    run_id = f"{RUN_PREFIX}-dupinvalid"
    rows = [
        build__row("dupinvalid", max_price="2001"),
        build__row("dupinvalid", max_price="2002"),
        build__row("dupinvalid", min_price="abc"),
    ]
    insert__bronze(run_id, rows)
    run__silver(run_id)

    clean = get__clean(run_id)
    assert len(clean) == 1
    assert clean[0]["max_price"] == Decimal("2002.00")
    assert [(r["row_no"], r["reject_rule"]) for r in get__rejected(run_id)] == [
        (1, "duplicate"),
        (3, "invalid_price"),
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: every bronze row ends up in one place

# COMMAND ----------

def test__row_counts_add_up():
    """clean + rejected equals the bronze rows of the run, on a mixed batch."""
    run_id = f"{RUN_PREFIX}-counts"
    rows = [
        build__row("counts-a"),
        build__row("counts-b"),
        build__row("counts-c"),
        build__row("counts-d", min_price="abc"),
        build__row("counts-e"),
        build__row("counts-e"),
    ]
    insert__bronze(run_id, rows)
    run__silver(run_id)

    bronze_count = spark.table(BRONZE).where(f"_run_id = '{run_id}'").count()
    assert bronze_count == len(rows)
    assert len(get__clean(run_id)) == 4
    assert len(get__rejected(run_id)) == 2
    assert len(get__clean(run_id)) + len(get__rejected(run_id)) == bronze_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: reruns

# COMMAND ----------

def test__rerun_does_not_duplicate_rows():
    """Running the same run twice leaves the same rows in clean and in rejected."""
    run_id = f"{RUN_PREFIX}-rerun"
    insert__bronze(run_id, [build__row("rerun"), build__row("rerun-bad", min_price="abc")])
    run__silver(run_id)
    run__silver(run_id)

    assert len(get__clean(run_id)) == 1
    assert len(get__rejected(run_id)) == 1


def test__rerun_keeps_changed_rejected_status():
    """A rejected row whose status was changed by hand keeps it when the run is processed again."""
    run_id = f"{RUN_PREFIX}-keepstatus"
    insert__bronze(run_id, [build__row("keepstatus", min_price="abc")])
    run__silver(run_id)
    spark.sql(f"UPDATE {REJECTED} SET status = 'RESOLVED' WHERE run_id = '{run_id}'")
    run__silver(run_id)

    rejected = get__rejected(run_id)
    assert len(rejected) == 1
    assert rejected[0]["status"] == "RESOLVED"


def test__new_run_updates_existing_clean_row():
    """A later run with the same natural key updates the clean row and its run_id; no second row is added."""
    first_run = f"{RUN_PREFIX}-update1"
    second_run = f"{RUN_PREFIX}-update2"
    variety = f"{RUN_PREFIX}-update"

    insert__bronze(first_run, [build__row("update", max_price="2000")])
    run__silver(first_run)
    insert__bronze(second_run, [build__row("update", max_price="2500", modal_price="1800")])
    run__silver(second_run)

    rows = spark.table(CLEAN).where(f"variety = '{variety}'").collect()
    assert len(rows) == 1
    assert rows[0]["run_id"] == second_run
    assert rows[0]["max_price"] == Decimal("2500.00")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: errors

# COMMAND ----------

def test__blank_run_id_fails():
    """A blank run_id raises."""
    with pytest.raises(Exception):
        run__silver("")


def test__run_id_without_bronze_rows_fails():
    """A run_id with no rows in bronze raises and writes nothing."""
    run_id = f"{RUN_PREFIX}-norows"

    with pytest.raises(Exception):
        run__silver(run_id)

    assert len(get__clean(run_id)) == 0
    assert len(get__rejected(run_id)) == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
