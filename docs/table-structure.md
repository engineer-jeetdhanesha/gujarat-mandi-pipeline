# Table structure

Catalog `gujarat_mandi_pipeline_<env>_ws`, schemas `bronze`, `silver` and `gold`.

## Bronze

### bronze.raw_mandi_transactions
- Price rows exactly as received from the CSV file, all text.

| Column | Type | Description |
|---|---|---|
| state | STRING | State |
| district | STRING | District |
| market | STRING | Market (APMC yard) |
| commodity | STRING | Commodity name |
| commodity_code | STRING | Commodity code |
| variety | STRING | Variety |
| grade | STRING | Grade |
| arrival_date | STRING | Date, `dd/MM/yyyy` |
| min_price | STRING | Lowest price, rupees per quintal |
| max_price | STRING | Highest price, rupees per quintal |
| modal_price | STRING | Most common price, rupees per quintal |
| _ingested_at | TIMESTAMP | Load time |
| _source_mode | STRING | Always `FILE` |
| _file_name | STRING | Source file |
| _run_id | STRING | Run that loaded the row |
| _row_no | BIGINT | Row number in the file |

### bronze.raw_dim_market, bronze.raw_dim_commodity, bronze.raw_dim_date
- The three seed files exactly as received, all text.

| Column | Type | Description |
|---|---|---|
| dimension columns | STRING | Same as the silver dimension, without `updated_at` |
| _ingested_at | TIMESTAMP | Load time |
| _source_mode | STRING | Always `FILE` |
| _file_name | STRING | Source file |
| _run_id | STRING | Run that loaded the row |
| _row_no | BIGINT | Row number in the file |

## Silver

### silver.mandi_prices_clean
- Price rows that passed all data quality rules, typed and standardised.

| Column | Type | Description |
|---|---|---|
| state | STRING | State |
| district | STRING | District as in the file |
| district_std | STRING | Standard district name from `dim_market` |
| market | STRING | Market |
| commodity_code | INT | Commodity code |
| commodity | STRING | Standard commodity name from `dim_commodity` |
| variety | STRING | Variety |
| grade | STRING | Grade |
| arrival_date | DATE | Date of the price |
| min_price | DECIMAL(10,2) | Lowest price, rupees per quintal |
| max_price | DECIMAL(10,2) | Highest price, rupees per quintal |
| modal_price | DECIMAL(10,2) | Most common price, rupees per quintal |
| run_id | STRING | Run that last wrote the row |
| ingested_at | TIMESTAMP | Time the row was loaded into bronze |
| updated_at | TIMESTAMP | Last update time |

### silver.mandi_prices_rejected
- Price rows that failed a rule, with the rule and the reason.

| Column | Type | Description |
|---|---|---|
| state ... modal_price | STRING | The 11 raw columns of the file |
| source_mode, file_name, run_id, ingested_at | as bronze | Where the row came from |
| row_no | BIGINT | Row number in the file |
| reject_rule | STRING | Ids of the failed rules, comma separated |
| reject_reason | STRING | Reasons, separated by `;` |
| rejected_at | TIMESTAMP | Time of rejection |
| status | STRING | `ERROR` (failed a rule) or `REJECTED` (duplicate) |
| updated_at | TIMESTAMP | Last update time |

### silver.dim_market
- One row per market (APMC) with its standard district name.

| Column | Type | Description |
|---|---|---|
| market_key | BIGINT | Surrogate key |
| state | STRING | State |
| district_name | STRING | Standard district name |
| market_name | STRING | Market name |
| updated_at | TIMESTAMP | Last load time |

### silver.dim_commodity
- One row per commodity.

| Column | Type | Description |
|---|---|---|
| commodity_key | BIGINT | Surrogate key |
| commodity_code | INT | Commodity code |
| commodity_name | STRING | Standard name |
| category | STRING | Cereal, pulse, oilseed, vegetable, fruit, spice or cash crop |
| is_perishable | BOOLEAN | True for fresh produce that spoils quickly |
| updated_at | TIMESTAMP | Last load time |

### silver.dim_date
- One row per calendar day.

| Column | Type | Description |
|---|---|---|
| date_key | INT | Date as `yyyyMMdd` |
| date | DATE | Calendar date |
| day | INT | Day of the month |
| day_name | STRING | Monday to Sunday |
| week_start | DATE | Monday of the week |
| month | INT | Month number |
| month_name | STRING | January to December |
| quarter | INT | Quarter, 1 to 4 |
| year | INT | Year |
| is_weekend | BOOLEAN | True for Saturday and Sunday |
| season | STRING | Kharif, Rabi or Zaid |
| updated_at | TIMESTAMP | Last load time |

### silver.fact_mandi_prices
- Transaction fact table: one row per market, commodity, variety, grade and date.

| Column | Type | Description |
|---|---|---|
| market_key | BIGINT | Foreign key to `dim_market` |
| commodity_key | BIGINT | Foreign key to `dim_commodity` |
| date_key | INT | Foreign key to `dim_date` |
| variety | STRING | Degenerate dimension |
| grade | STRING | Degenerate dimension |
| min_price | DECIMAL(10,2) | Lowest price, rupees per quintal |
| max_price | DECIMAL(10,2) | Highest price, rupees per quintal |
| modal_price | DECIMAL(10,2) | Most common price, rupees per quintal |
| loaded_at | TIMESTAMP | Load time |

## Gold

### gold.mart_price_weekly
- Weekly prices per district and commodity, read by the dashboard.

| Column | Type | Description |
|---|---|---|
| year | INT | ISO year of the week |
| week_number | INT | ISO week number |
| week_start | DATE | Monday of the week |
| district_name | STRING | Standard district name |
| commodity_name | STRING | Standard commodity name |
| category | STRING | Commodity category |
| min_price | DECIMAL(10,2) | Lowest min price of the week |
| max_price | DECIMAL(10,2) | Highest max price of the week |
| avg_modal_price | DECIMAL(10,2) | Average modal price of the week |
| is_complete_week | BOOLEAN | False if the loaded data does not cover the whole week |
| updated_at | TIMESTAMP | Last update time |
