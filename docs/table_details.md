# Data Model

## Bronze

### `bronze.mandi_raw`

Raw mandi prices from the data.gov.in API (daily) or a CSV file (backfill), stored exactly as received. File loads read every `/Volumes/<catalog>/bronze/raw_files/mandi_*.csv` (volume created by `setup_bronze`).

| # | Column | Type | Source | Description |
|---|---|---|---|---|
| 1 | State | STRING | API / file | State name (always `Gujarat`) |
| 2 | District | STRING | API / file | District name, raw (cleaned in silver) |
| 3 | Market | STRING | API / file | APMC name, raw (cleaned in silver) |
| 4 | Commodity | STRING | API / file | Commodity name |
| 5 | Commodity_Code | STRING | API / file | Commodity code |
| 6 | Variety | STRING | API / file | Commodity variety |
| 7 | Grade | STRING | API / file | Quality grade (e.g. `FAQ`) |
| 8 | Arrival_Date | STRING | API / file | Arrival date, `dd/mm/yyyy` |
| 9 | Min_Price | STRING | API / file | Minimum price (Rs/quintal) |
| 10 | Max_Price | STRING | API / file | Maximum price (Rs/quintal) |
| 11 | Modal_Price | STRING | API / file | Modal (most common) price (Rs/quintal) |
| 12 | _source | STRING | pipeline | `api` or `file` |
| 13 | _source_file | STRING | pipeline | CSV path for file loads, NULL for API loads |
| 14 | _ingested_at | TIMESTAMP | pipeline | Time the row was loaded |
| 15 | _run_id | STRING | pipeline | Databricks job run ID (joins to `audit.pipeline_run_log.run_id`) |

**Rules**
- Append only; duplicates are removed in silver.
- All source columns are `STRING`; typing happens in silver.
- No partitioning (small table). Add `CLUSTER BY` later if needed.
- `TBLPROPERTIES ('delta.columnMapping.mode' = 'name')`

## Audit

### `audit.pipeline_run_log`

One row per pipeline run, covering the whole flow from bronze to gold.

| # | Column | Type | Example | Description |
|---|---|---|---|---|
| 1 | run_id | STRING | `1048223` | Databricks job run ID |
| 2 | pipeline_name | STRING | `mandi_daily` | Pipeline name (e.g. `mandi_daily`, `mandi_backfill`) |
| 3 | run_date | DATE | `2026-09-02` | Date the pipeline ran |
| 4 | data_start_date | DATE | `2026-09-01` | First `Arrival_Date` processed |
| 5 | data_end_date | DATE | `2026-09-01` | Last `Arrival_Date` processed (same as start for a single day) |
| 6 | ingest_mode | STRING | `API` | `API` or `FILE` |
| 7 | status | STRING | `SUCCESS` | `RUNNING`, `SUCCESS` or `FAILED` |
| 8 | started_at | TIMESTAMP | `2026-09-02 06:00:03` | Run start time |
| 9 | ended_at | TIMESTAMP | `2026-09-02 06:14:40` | Run end time (NULL while running) |

**Rules**
- A run inserts a `RUNNING` row at start, then updates it to `SUCCESS` or `FAILED` and sets `ended_at` at the end.
- A `RUNNING` row older than a few hours is treated as failed (the job crashed).
- Error details are in the Databricks run page, found through `run_id`.
- `TBLPROPERTIES ('delta.columnMapping.mode' = 'name')`

## Silver

**Load order per run:** `dim_district` → `dim_market` → `dim_commodity` → `dim_date` → `mandi_price` + `mandi_price_rejected`

**Name mapping (districts and markets)**
- One-time setup: analyse all existing raw names and load one dim row per raw name, mapped to its clean name (`mapped_by = 'initial'`).
- Daily: look up the raw name exactly. If it is not found, call `ai_query` once to map it to an existing clean name (or a new one), and insert it as a new dim row (`mapped_by = 'ai'`). From then on it is an exact match.
- Several raw names can share one clean name, so gold groups by `market_name` / `district_name`, not by key.

### `silver.dim_district`

| # | Column | Type | Example | Description |
|---|---|---|---|---|
| 1 | district_key | INT | `31` | Surrogate key |
| 2 | raw_district_name | STRING | `Vadodara(Baroda)` | District name exactly as in bronze |
| 3 | district_name | STRING | `Vadodara` | Clean district name |
| 4 | region | STRING | `Central Gujarat` | Region of Gujarat |
| 5 | mapped_by | STRING | `initial` | `initial` or `ai` |
| 6 | updated_at | TIMESTAMP | `2026-09-02 06:01:00` | Last update time |

### `silver.dim_market`

| # | Column | Type | Example | Description |
|---|---|---|---|---|
| 1 | market_key | INT | `140` | Surrogate key |
| 2 | raw_market_name | STRING | `JASDAN APMC` | Market name exactly as in bronze |
| 3 | market_name | STRING | `Jasdan` | Clean APMC name |
| 4 | yard_name | STRING | `Vichhiya` | Sub-yard name, NULL for the main yard |
| 5 | district_key | INT | `24` | District of the market |
| 6 | mapped_by | STRING | `ai` | `initial` or `ai` |
| 7 | updated_at | TIMESTAMP | `2026-09-02 06:01:05` | Last update time |

### `silver.dim_commodity`

| # | Column | Type | Example | Description |
|---|---|---|---|---|
| 1 | commodity_code | INT | `81` | Commodity code from the source (natural key) |
| 2 | commodity_name | STRING | `Bitter Gourd` | One clean name per code |
| 3 | category | STRING | `Vegetable` | Vegetable, fruit, oilseed, spice, grain, etc. |
| 4 | updated_at | TIMESTAMP | `2026-09-02 06:01:08` | Last update time |

### `silver.dim_date`

Calendar generated in code.

| # | Column | Type | Example |
|---|---|---|---|
| 1 | date | DATE | `2026-09-01` |
| 2 | week_start | DATE | `2026-08-31` |
| 3 | month | INT | `9` |
| 4 | month_name | STRING | `September` |
| 5 | year | INT | `2026` |
| 6 | season | STRING | `Kharif` |
| 7 | is_weekend | BOOLEAN | `false` |

### `silver.mandi_price`

Cleaned, typed and deduplicated prices. Loaded with `MERGE` on `market_key + commodity_code + variety + grade + arrival_date`; API rows win over file rows.

| # | Column | Type | Example | Description |
|---|---|---|---|---|
| 1 | arrival_date | DATE | `2026-09-01` | Parsed from `Arrival_Date` (`dd/mm/yyyy`) |
| 2 | market_key | INT | `140` | From `dim_market` |
| 3 | commodity_code | INT | `81` | From `dim_commodity` |
| 4 | variety | STRING | `Other` | Trimmed |
| 5 | grade | STRING | `FAQ` | Trimmed |
| 6 | min_price | DECIMAL(10,2) | `550.00` | Rs/quintal |
| 7 | max_price | DECIMAL(10,2) | `600.00` | Rs/quintal |
| 8 | modal_price | DECIMAL(10,2) | `580.00` | Rs/quintal |
| 9 | _source | STRING | `api` | `api` or `file` |
| 10 | _run_id | STRING | `1048223` | Run that last wrote the row |
| 11 | _updated_at | TIMESTAMP | `2026-09-02 06:03:40` | Last MERGE time |

### `silver.mandi_price_rejected`

Bronze rows whose content fails validation. They are kept here instead of being dropped, retried on every run, and deleted once they pass.

| # | Column | Type | Example | Description |
|---|---|---|---|---|
| 1–11 | State … Modal_Price | STRING | | Raw values copied from bronze |
| 12 | reject_reasons | ARRAY<STRING> | `["MIN_GT_MAX"]` | Every rule the row failed |
| 13 | _source | STRING | `api` | `api` or `file` |
| 14 | _run_id | STRING | `1048223` | Run that rejected the row |
| 15 | _rejected_at | TIMESTAMP | `2026-09-02 06:03:40` | Time of rejection |

**Reject reasons**

| Code | Rule |
|---|---|
| `BAD_DATE` | `Arrival_Date` does not parse as `dd/mm/yyyy` |
| `BAD_PRICE` | A price does not cast to a number, or is ≤ 0 |
| `MIN_GT_MAX` | `Min_Price > Max_Price` |
| `MODAL_OUT_OF_RANGE` | `Modal_Price` is not between min and max |

## Gold

Denormalized marts for the dashboard: names instead of keys, markets grouped by `market_name`.

### `gold.weekly_market_price`

How prices move week on week. **Grain:** week × market × commodity.

| # | Column | Type | Example |
|---|---|---|---|
| 1 | week_start | DATE | `2026-08-31` |
| 2 | district_name | STRING | `Rajkot` |
| 3 | region | STRING | `Saurashtra` |
| 4 | market_name | STRING | `Gondal` |
| 5 | commodity_name | STRING | `Groundnut` |
| 6 | category | STRING | `Oilseed` |
| 7 | avg_modal_price | DECIMAL(10,2) | `6250.00` |
| 8 | min_price | DECIMAL(10,2) | `5800.00` |
| 9 | max_price | DECIMAL(10,2) | `6900.00` |
| 10 | trading_days | INT | `5` |
| 11 | prev_week_avg_modal_price | DECIMAL(10,2) | `6000.00` |
| 12 | wow_change_pct | DECIMAL(6,2) | `4.17` |
| 13 | _updated_at | TIMESTAMP | |

### `gold.best_market`

Where a commodity gets the best price. **Grain:** week × commodity × market (ranked).

| # | Column | Type | Example |
|---|---|---|---|
| 1 | week_start | DATE | `2026-08-31` |
| 2 | commodity_name | STRING | `Groundnut` |
| 3 | category | STRING | `Oilseed` |
| 4 | market_name | STRING | `Gondal` |
| 5 | district_name | STRING | `Rajkot` |
| 6 | avg_modal_price | DECIMAL(10,2) | `6250.00` |
| 7 | price_rank | INT | `1` |
| 8 | pct_above_state_avg | DECIMAL(6,2) | `8.50` |
| 9 | _updated_at | TIMESTAMP | |

### `gold.price_volatility`

Which prices are unstable. **Grain:** as_of_date × market × commodity (rolling 30 days).

| # | Column | Type | Example |
|---|---|---|---|
| 1 | as_of_date | DATE | `2026-09-01` |
| 2 | market_name | STRING | `Gondal` |
| 3 | district_name | STRING | `Rajkot` |
| 4 | commodity_name | STRING | `Groundnut` |
| 5 | mean_modal_price | DECIMAL(10,2) | `6100.00` |
| 6 | stddev_modal_price | DECIMAL(10,2) | `320.00` |
| 7 | volatility_ratio | DECIMAL(6,4) | `0.0525` |
| 8 | trading_days | INT | `22` |
| 9 | _updated_at | TIMESTAMP | |

### `gold.commodity_seasonal_trend`

When prices are highest for each commodity. **Grain:** month × commodity (state level).

| # | Column | Type | Example |
|---|---|---|---|
| 1 | year | INT | `2026` |
| 2 | month | INT | `9` |
| 3 | season | STRING | `Kharif` |
| 4 | commodity_name | STRING | `Groundnut` |
| 5 | category | STRING | `Oilseed` |
| 6 | avg_modal_price | DECIMAL(10,2) | `6150.00` |
| 7 | mom_change_pct | DECIMAL(6,2) | `-2.30` |
| 8 | markets_reporting | INT | `38` |
| 9 | total_price_records | INT | `1240` |
| 10 | _updated_at | TIMESTAMP | |

### `gold.dq_daily_coverage`

Data completeness and freshness for the dashboard health tile. **Grain:** arrival_date.

| # | Column | Type | Example |
|---|---|---|---|
| 1 | arrival_date | DATE | `2026-09-01` |
| 2 | rows_in_bronze | INT | `927` |
| 3 | rows_in_silver | INT | `925` |
| 4 | rows_rejected | INT | `2` |
| 5 | markets_reporting | INT | `142` |
| 6 | new_markets_ai_mapped | INT | `1` |
| 7 | last_run_status | STRING | `SUCCESS` |
| 8 | last_run_at | TIMESTAMP | `2026-09-02 06:14:40` |
| 9 | _updated_at | TIMESTAMP | |
