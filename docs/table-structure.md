# Gujarat Mandi Pipeline — Table Structure

Mandi prices only (weather is out of scope for now). Catalog `mandi_dev` / `mandi_prod`; schemas `bronze`, `silver`, `gold`. Source mode values everywhere: `API | FILE | BACKFILL`. The star schema (dimensions and fact) lives in Silver; Gold is for data marts. Variety and Grade are degenerate dimensions on the fact table. The audit table lives in Bronze.

---
## 1. Bronze

### bronze.raw_mandi_transactions — raw, append-only, all STRING
| Column | Type | Description |
|---|---|---|
| state | STRING | As received (always Gujarat) |
| district | STRING | Raw district spelling (e.g. Banaskanth) |
| market | STRING | APMC yard name, raw |
| commodity | STRING | Commodity name, raw |
| commodity_code | STRING | Agmarknet commodity code |
| variety | STRING | Variety, raw ("Other" is common) |
| grade | STRING | Grade, raw (FAQ, Local, Medium…) |
| arrival_date | STRING | `dd/MM/yyyy` as received |
| min_price | STRING | ₹/quintal, raw (may be decimal) |
| max_price | STRING | ₹/quintal, raw |
| modal_price | STRING | ₹/quintal, raw |
| _ingested_at | TIMESTAMP | Load time |
| _source_mode | STRING | API / FILE / BACKFILL |
| _file_name | STRING | Source file (NULL for API) |
| _run_id | STRING | Run that loaded the row (joins audit) |
| _row_no | BIGINT | Row number within the file/API page |

### bronze.pipeline_audit — one row per pipeline run
| Column | Type | Description |
|---|---|---|
| run_id | STRING | Unique run id (PK) |
| pipeline_name | STRING | Job/pipeline name |
| source_mode | STRING | API / FILE / BACKFILL |
| business_start_date | DATE | First arrival_date covered by the run (daily run: = end date; backfill: range start) |
| business_end_date | DATE | Last arrival_date covered by the run |
| status | STRING | RUNNING / SUCCESS / FAILED |
| source_total_rows | BIGINT | Rows received from the source (API total or file row count) |
| source_rows_rejected | BIGINT | Rows from this run sent to silver.mandi_prices_rejected |
| start_timestamp | TIMESTAMP | Run start |
| end_timestamp | TIMESTAMP | Run end (NULL while RUNNING) |
| error_message | STRING | Failure detail (NULL on success) |

---
## 2. Silver

### silver.mandi_prices_clean — typed, standardised, deduped (MERGE target)
Natural key: `market + commodity_code + variety + grade + arrival_date`
| Column | Type | Description |
|---|---|---|
| state | STRING | Trimmed |
| district | STRING | Raw district (trimmed) |
| district_std | STRING | Standard district name from `silver.dim_market` |
| market | STRING | Trimmed market name |
| commodity_code | INT | Commodity code (natural key of `dim_commodity`) |
| commodity | STRING | Standard name from `silver.dim_commodity` (one canonical name per code) |
| variety | STRING | Trimmed variety |
| grade | STRING | Trimmed grade |
| arrival_date | DATE | Parsed from dd/MM/yyyy |
| min_price | DECIMAL(10,2) | ₹/quintal |
| max_price | DECIMAL(10,2) | ₹/quintal |
| modal_price | DECIMAL(10,2) | ₹/quintal |
| run_id | STRING | Run that last wrote the row |
| ingested_at | TIMESTAMP | Original bronze ingest time |
| updated_at | TIMESTAMP | Last MERGE time |

### silver.mandi_prices_rejected — failed DQ rules, raw values kept
| Column | Type | Description |
|---|---|---|
| state … modal_price | STRING | Same 11 raw columns as bronze (STRING so bad dates/prices still fit) |
| source_mode, file_name, run_id, ingested_at | as bronze | Lineage back to bronze |
| reject_reason | STRING | Human-readable reason |
| reject_rule | STRING | Rule id (null/unparseable date or price, price ≤ 0, min>max, modal outside min–max, missing market/code, unknown market, unknown commodity code, future arrival date) |
| rejected_at | TIMESTAMP | When rejected |
| status | STRING | ERROR (pending decision) / RESOLVED (fixed, ready to reload) / REJECTED (stays out) / LOADED (repaired and merged into clean) |
| updated_at | TIMESTAMP | Last status change |

### silver.dim_market — one row per APMC market (seed: `seed/dim_market.csv`)
Natural key: `market_name`
| Column | Type | Description |
|---|---|---|
| market_key | BIGINT | Surrogate key |
| state | STRING | Gujarat |
| district_name | STRING | Standard district name |
| market_name | STRING | Market (APMC) name |
| updated_at | TIMESTAMP | Last seed load |

### silver.dim_commodity — one row per commodity code (seed: `seed/dim_commodity.csv`)
| Column | Type | Description |
|---|---|---|
| commodity_key | BIGINT | Surrogate key |
| commodity_code | INT | Natural key |
| commodity_name | STRING | Standard name |
| category | STRING | Cereal / pulse / oilseed / vegetable / fruit / spice / cash crop |
| is_perishable | BOOLEAN | Fresh produce that spoils quickly |
| updated_at | TIMESTAMP | Last seed load |

### silver.dim_date — one row per calendar day (seed: `seed/dim_date.csv`, 2026)
| Column | Type | Description |
|---|---|---|
| date_key | INT | yyyyMMdd |
| date | DATE | Calendar date (natural key) |
| day | INT | Day of month |
| day_name | STRING | Monday … Sunday |
| week_start | DATE | Monday of the week |
| month | INT | Month number |
| month_name | STRING | January … December |
| quarter | INT | Calendar quarter 1–4 |
| year | INT | Calendar year |
| is_weekend | BOOLEAN | Sat/Sun |
| season | STRING | Kharif (Jun–Oct) / Rabi (Nov–Mar) / Zaid (Apr–May) |
| updated_at | TIMESTAMP | Last seed load |

The dimension tables are loaded from the CSV files in `seed/` (same columns as the table, except `updated_at`) with INSERT OVERWRITE, so the CSV is the source of truth. A new market or commodity is added by editing the CSV and loading it again. A row whose market or commodity code is not in the dimensions is rejected in Silver.

### silver.fact_mandi_prices — grain: market × commodity × variety × grade × date (derived from clean)
| Column | Type | Description |
|---|---|---|
| market_key | BIGINT | FK → dim_market |
| commodity_key | BIGINT | FK → dim_commodity |
| date_key | INT | FK → dim_date |
| variety | STRING | Degenerate dim |
| grade | STRING | Degenerate dim |
| min_price, max_price, modal_price | DECIMAL(10,2) | ₹/quintal measures |
| loaded_at | TIMESTAMP | Fact load time |

---
## 3. Gold (data marts)
Aggregated tables for the dashboard, built from the Silver star schema. To be designed.
