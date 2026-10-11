# Gujarat Mandi Pipeline — Table Structure

Mandi prices only (weather is out of scope for now). Catalog `mandi_dev` / `mandi_prod`; schemas `bronze`, `silver`, `gold`. Source mode values everywhere: `API | FILE | BACKFILL`. Variety and Grade are degenerate dimensions on the fact table. The audit table lives in Bronze.

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
| district_std | STRING | Standard district via alias lookup |
| market | STRING | Trimmed market name |
| commodity_code | INT | Commodity code (the commodity key) |
| commodity | STRING | Trimmed name (strip `\n`; 6 codes have 2 spellings → one canonical name per code) |
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
| reject_rule | STRING | Rule id (null/unparseable date or price, price ≤ 0, min>max, modal outside min–max, missing market/code, unmatched district) |
| rejected_at | TIMESTAMP | When rejected |
| status | STRING | ERROR (pending decision) / RESOLVED / REJECTED |
| updated_at | TIMESTAMP | Last status change |

---
## 3. Gold (star schema)
### gold.dim_commodity
| Column | Type | Description |
|---|---|---|
| commodity_key | BIGINT | Surrogate key |
| commodity_code | INT | Natural key |
| commodity_name | STRING | Clean name |
| category | STRING | Oilseed / vegetable / fruit / spice… |
| updated_at | TIMESTAMP | Last refresh |

### gold.dim_market
| Column | Type | Description |
|---|---|---|
| market_key | BIGINT | Surrogate key |
| state | STRING | Gujarat |
| district_name | STRING | Standard district |
| market_name | STRING | Market (APMC) name |
| updated_at | TIMESTAMP | Last refresh (SCD1) |

### gold.dim_date
| Column | Type | Description |
|---|---|---|
| date_key | INT | yyyyMMdd |
| date | DATE | Calendar date |
| week_start | DATE | Monday of the week |
| month, year | INT | Calendar parts |
| is_weekend | BOOLEAN | Sat/Sun |
| season | STRING | Kharif / Rabi / Zaid |

### gold.fact_mandi_prices — grain: market × commodity × variety × grade × date
| Column | Type | Description |
|---|---|---|
| market_key | BIGINT | FK → dim_market |
| commodity_key | BIGINT | FK → dim_commodity |
| date_key | INT | FK → dim_date |
| variety | STRING | Degenerate dim |
| grade | STRING | Degenerate dim |
| min_price, max_price, modal_price | DECIMAL(10,2) | ₹/quintal measures |
| loaded_at | TIMESTAMP | Fact load time |
