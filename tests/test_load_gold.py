# Databricks notebook source
# MAGIC %md
# MAGIC # Test: gold weekly mart
# MAGIC
# MAGIC Runs `src/setup` and `src/load_silver_dims`, then runs `src/load_gold` as a child notebook on small dummy data, and checks
# MAGIC what lands in `gold.mart_price_weekly`: the aggregates, week-on-week change (including a missing week),
# MAGIC `is_complete_week`, that only the weeks a run touched (plus the week before and after) are rewritten, reruns,
# MAGIC and the errors for a blank or unknown `run_id`.
# MAGIC
# MAGIC Tests are plain pytest functions run inside this notebook with `ipytest`.
# MAGIC The notebook fails if any test fails, so it can be used as a job task.
# MAGIC
# MAGIC The dummy data uses its own district, markets and commodities (dimension keys from 900000000, names starting
# MAGIC with `pytest-`) and dates in December 2026, so it never mixes with real mart rows.
# MAGIC
# MAGIC Cleanup: after the tests, the dummy rows are deleted from the mart, the fact, clean and the dimensions.
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
DIM_MARKET = f"{test_catalog}.silver.dim_market"
DIM_COMMODITY = f"{test_catalog}.silver.dim_commodity"
CLEAN = f"{test_catalog}.silver.mandi_prices_clean"
FACT = f"{test_catalog}.silver.fact_mandi_prices"
MART = f"{test_catalog}.gold.mart_price_weekly"

# Tables and schemas that setup creates, tables first so they are dropped before their schema.
SETUP_TABLES = [
    MART,
    FACT,
    CLEAN,
    f"{test_catalog}.silver.mandi_prices_rejected",
    DIM_MARKET,
    DIM_COMMODITY,
    f"{test_catalog}.silver.dim_date",
    f"{test_catalog}.bronze.raw_mandi_transactions",
]
SETUP_SCHEMAS = [f"{test_catalog}.gold", f"{test_catalog}.silver", f"{test_catalog}.bronze"]

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

# MAGIC %run ../src/load_silver_dims

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
# MAGIC ## Dummy dimensions
# MAGIC
# MAGIC One dummy district with two markets. Each test gets its own dummy commodity, so mart rows never overlap.

# COMMAND ----------

from datetime import date, datetime, timedelta
from decimal import Decimal

DISTRICT = f"{RUN_PREFIX}-district"
MARKET_A = 900000001
MARKET_B = 900000002
FIRST_DUMMY_KEY = 900000000

# Mondays of December 2026 (dim_date covers 2026).
W1, W2, W3, W4 = date(2026, 12, 7), date(2026, 12, 14), date(2026, 12, 21), date(2026, 12, 28)

_commodity_counter = [0]

spark.createDataFrame(
    [
        (MARKET_A, "Gujarat", DISTRICT, f"{RUN_PREFIX}-market-a", datetime.now()),
        (MARKET_B, "Gujarat", DISTRICT, f"{RUN_PREFIX}-market-b", datetime.now()),
    ],
    spark.table(DIM_MARKET).schema,
).write.mode("append").saveAsTable(DIM_MARKET)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def new__commodity() -> dict[str, object]:
    """Add a dummy commodity to dim_commodity and return its key, code and name."""
    _commodity_counter[0] += 1
    n = _commodity_counter[0]
    commodity = {
        "key": FIRST_DUMMY_KEY + n,
        "code": 900000 + n,
        "name": f"{RUN_PREFIX}-commodity-{n}",
        "category": "Pytest",
    }
    spark.createDataFrame(
        [(commodity["key"], commodity["code"], commodity["name"], commodity["category"], False, datetime.now())],
        spark.table(DIM_COMMODITY).schema,
    ).write.mode("append").saveAsTable(DIM_COMMODITY)
    return commodity

# COMMAND ----------

def add__week(
    run_id: str, commodity: dict[str, object], monday: date, prices: list[tuple[str, str, str, str]], touch: bool = True
) -> None:
    """Add fact rows for one week, and a clean row for the run so the week counts as touched.

    Args:
        run_id: Run id written on the clean row.
        commodity: Dict from new__commodity.
        monday: Monday of the week; all fact rows are dated that day.
        prices: List of (market_key, min_price, max_price, modal_price); one fact row each, with
            a different variety per row.
        touch: False to add the fact rows only (the week is not part of the run).
    """
    fact_schema = spark.table(FACT).schema
    date_key = int(monday.strftime("%Y%m%d"))
    fact_rows = []
    for i, (market_key, low, high, modal) in enumerate(prices):
        values = {
            "market_key": market_key,
            "commodity_key": commodity["key"],
            "date_key": date_key,
            "variety": f"{RUN_PREFIX}-{commodity['code']}-{i}",
            "grade": "FAQ",
            "min_price": Decimal(low),
            "max_price": Decimal(high),
            "modal_price": Decimal(modal),
            "loaded_at": datetime.now(),
        }
        fact_rows.append(tuple(values[f.name] for f in fact_schema.fields))
    spark.createDataFrame(fact_rows, fact_schema).write.mode("append").saveAsTable(FACT)

    if touch:
        clean_schema = spark.table(CLEAN).schema
        clean = {
            "state": "Gujarat",
            "district": DISTRICT,
            "district_std": DISTRICT,
            "market": f"{RUN_PREFIX}-market-a",
            "commodity_code": commodity["code"],
            "commodity": commodity["name"],
            "variety": f"{RUN_PREFIX}-{commodity['code']}-touch",
            "grade": "FAQ",
            "arrival_date": monday,
            "min_price": Decimal("1.00"),
            "max_price": Decimal("2.00"),
            "modal_price": Decimal("1.50"),
            "run_id": run_id,
            "ingested_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        spark.createDataFrame([tuple(clean[f.name] for f in clean_schema.fields)], clean_schema).write.mode(
            "append"
        ).saveAsTable(CLEAN)

# COMMAND ----------

def run__gold(run_id: str) -> str:
    """Run src/load_gold as a child notebook; raises if the notebook itself fails (blank widget, no catalog).

    Args:
        run_id: Value for the run_id widget.

    Returns:
        The notebook's exit value (empty, gold does not call exit).
    """
    return dbutils.notebook.run("../src/load_gold", 600, {"run_id": run_id})

# COMMAND ----------

def get__mart(commodity: dict[str, object]) -> dict[date, "Row"]:
    """Return the mart rows of a dummy commodity as a dict of week_start -> Row."""
    rows = spark.table(MART).where(f"commodity_code = {commodity['code']}").collect()
    return {r["week_start"]: r for r in rows}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

@pytest.fixture(scope="module", autouse=True)
def clean_up() -> "Generator[None, None, None]":
    """After the tests, delete the dummy rows; drop the tables/schemas only if this run created them.

    No CASCADE: dropping a schema fails instead of removing anything else in it.
    """
    yield

    spark.sql(f"DELETE FROM {MART} WHERE district_name = '{DISTRICT}'")
    spark.sql(f"DELETE FROM {FACT} WHERE market_key >= {FIRST_DUMMY_KEY}")
    spark.sql(f"DELETE FROM {CLEAN} WHERE run_id LIKE '{RUN_PREFIX}%'")
    spark.sql(f"DELETE FROM {DIM_MARKET} WHERE market_key >= {FIRST_DUMMY_KEY}")
    spark.sql(f"DELETE FROM {DIM_COMMODITY} WHERE commodity_key >= {FIRST_DUMMY_KEY}")

    for name in SETUP_TABLES:
        if not tables_existed[name]:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
    for name in SETUP_SCHEMAS:
        if not schemas_existed[name]:
            spark.sql(f"DROP SCHEMA IF EXISTS {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: aggregates

# COMMAND ----------

def test__weekly_aggregates() -> None:
    """One week with three fact rows from two markets gives the right min, max, average, sum and counts."""
    run_id = f"{RUN_PREFIX}-agg"
    c = new__commodity()
    add__week(run_id, c, W1, [(MARKET_A, "100", "200", "150"), (MARKET_A, "90", "250", "200"), (MARKET_B, "120", "210", "160")])
    run__gold(run_id)

    mart = get__mart(c)
    assert list(mart) == [W1]
    r = mart[W1]
    iso = W1.isocalendar()
    assert (r["year"], r["week_number"]) == (iso[0], iso[1])
    assert (r["district_name"], r["commodity_code"], r["commodity_name"], r["category"]) == (
        DISTRICT, c["code"], c["name"], "Pytest",
    )
    assert (r["min_price"], r["max_price"]) == (Decimal("90.00"), Decimal("250.00"))
    assert r["price_records"] == 3
    assert r["sum_modal_price"] == Decimal("510.00")
    assert r["avg_modal_price"] == Decimal("170.00")
    assert r["markets_count"] == 2
    assert r["prev_week_avg_price"] is None and r["wow_change_pct"] is None
    assert r["updated_at"] is not None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: week-on-week change and complete weeks

# COMMAND ----------

def test__week_on_week_change_and_missing_week() -> None:
    """The change compares with the week exactly 7 days earlier; a missing week gives NULL, not an older week."""
    run_id = f"{RUN_PREFIX}-wow"
    c = new__commodity()
    add__week(run_id, c, W1, [(MARKET_A, "90", "110", "100")])
    add__week(run_id, c, W2, [(MARKET_A, "140", "160", "150")])
    add__week(run_id, c, W4, [(MARKET_A, "110", "130", "120")])
    run__gold(run_id)

    mart = get__mart(c)
    assert sorted(mart) == [W1, W2, W4]
    assert mart[W1]["prev_week_avg_price"] is None and mart[W1]["wow_change_pct"] is None
    assert mart[W2]["prev_week_avg_price"] == Decimal("100.00")
    assert mart[W2]["wow_change_pct"] == Decimal("50.00")
    # W3 has no data, so W4 has no previous week.
    assert mart[W4]["prev_week_avg_price"] is None and mart[W4]["wow_change_pct"] is None


def test__is_complete_week() -> None:
    """A week that ends after the last loaded date is incomplete; earlier full weeks are complete."""
    run_id = f"{RUN_PREFIX}-complete"
    c = new__commodity()
    add__week(run_id, c, W1, [(MARKET_A, "90", "110", "100")])
    add__week(run_id, c, W2, [(MARKET_A, "90", "110", "100")])
    add__week(run_id, c, W4, [(MARKET_A, "90", "110", "100")])
    run__gold(run_id)

    mart = get__mart(c)
    assert mart[W1]["is_complete_week"] is True
    assert mart[W2]["is_complete_week"] is True
    # The last loaded date is W4 (a Monday), so W4 ends after it.
    assert mart[W4]["is_complete_week"] is False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: only the affected weeks are rewritten

# COMMAND ----------

def test__only_affected_weeks_are_rewritten() -> None:
    """A run touching one week rewrites that week, the week before and the week after, and nothing else."""
    first_run = f"{RUN_PREFIX}-inc1"
    second_run = f"{RUN_PREFIX}-inc2"
    c = new__commodity()
    for monday, modal in ((W1, "100"), (W2, "150"), (W3, "180"), (W4, "200")):
        add__week(first_run, c, monday, [(MARKET_A, "50", "300", modal)])
    run__gold(first_run)
    before = get__mart(c)
    assert sorted(before) == [W1, W2, W3, W4]

    # W2 changes: only the second run touches it.
    spark.sql(f"""
        UPDATE {FACT}
        SET modal_price = 300.00
        WHERE commodity_key = {c['key']} AND date_key = {int(W2.strftime('%Y%m%d'))}
    """)
    add__week(second_run, c, W2, [], touch=True)
    run__gold(second_run)
    after = get__mart(c)

    assert after[W2]["avg_modal_price"] == Decimal("300.00")
    assert after[W2]["updated_at"] > before[W2]["updated_at"]
    # The week before is rewritten (same values); the week after gets the new previous week.
    assert after[W1]["updated_at"] > before[W1]["updated_at"]
    assert after[W1]["avg_modal_price"] == Decimal("100.00")
    assert after[W3]["updated_at"] > before[W3]["updated_at"]
    assert after[W3]["prev_week_avg_price"] == Decimal("300.00")
    assert after[W3]["wow_change_pct"] == Decimal("-40.00")
    # Two weeks away: not touched.
    assert after[W4]["updated_at"] == before[W4]["updated_at"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: reruns

# COMMAND ----------

def test__rerun_does_not_duplicate_rows() -> None:
    """Running the same run twice leaves one row per week with the same values."""
    run_id = f"{RUN_PREFIX}-rerun"
    c = new__commodity()
    add__week(run_id, c, W1, [(MARKET_A, "90", "110", "100")])
    add__week(run_id, c, W2, [(MARKET_A, "140", "160", "150")])
    run__gold(run_id)
    first = {w: (r["avg_modal_price"], r["wow_change_pct"], r["price_records"]) for w, r in get__mart(c).items()}
    run__gold(run_id)
    second = {w: (r["avg_modal_price"], r["wow_change_pct"], r["price_records"]) for w, r in get__mart(c).items()}

    assert spark.table(MART).where(f"commodity_code = {c['code']}").count() == 2
    assert first == second

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: errors

# COMMAND ----------

def test__blank_run_id_fails() -> None:
    """A blank run_id raises."""
    with pytest.raises(Exception):
        run__gold("")


def test__run_id_without_clean_rows_fails() -> None:
    """A run_id with no rows in clean loads nothing."""
    before = spark.table(MART).count()

    run__gold(f"{RUN_PREFIX}-norows")

    assert spark.table(MART).count() == before

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

ipytest.run("-v")
