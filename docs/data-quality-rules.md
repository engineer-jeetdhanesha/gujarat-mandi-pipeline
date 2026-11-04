# Data quality rules

Two groups of rules: the **row rules** that decide whether a mandi price row reaches `silver.mandi_prices_clean`, and the **dimension checks** that decide whether a seed file is loaded. The code is in `src/lib_mandi/dq_checks.py`; every rule has a unit test in `tests/unit/test_dq_checks.py`.

## Row rules (silver transactions)

A row that fails any rule is written to `silver.mandi_prices_rejected` with its raw values, `reject_rule` (every failing rule id, comma separated, in the order below) and `reject_reason` (the reasons, separated by `;`). A row that passes all rules is typed and written to `silver.mandi_prices_clean`.

A rule that needs a valid value is skipped when that value is missing or invalid, so one bad value gives one reason.

| # | Rule id | Fails when | Reason text | Example |
|---|---|---|---|---|
| 1 | `missing_market` | the market is blank | Market is missing | `Market` = `` |
| 2 | `missing_commodity_code` | the commodity code is blank | Commodity code is missing | `Commodity_Code` = `` |
| 3 | `invalid_date` | the date is blank or not `dd/MM/yyyy` | Arrival date is missing or not dd/MM/yyyy | `2026-10-11` |
| 4 | `invalid_price` | a price is blank or not a number | A price is missing or not a number | `Min_Price` = `NA` |
| 5 | `price_le_zero` | a price is zero or negative | A price is zero or negative | `Min_Price` = `0` |
| 6 | `min_gt_max` | the minimum is above the maximum | Min price is greater than max price | min 900, max 800 |
| 7 | `modal_out_of_range` | the modal price is below the minimum or above the maximum | Modal price is outside the min-max range | min 100, max 200, modal 300 |
| 8 | `unknown_market` | the market is not in `dim_market` | Market is not in dim_market | a new APMC name |
| 9 | `unknown_commodity` | the commodity code is not in `dim_commodity` | Commodity code is not in dim_commodity | code 999 |
| 10 | `future_date` | the date is after today | Arrival date is in the future | tomorrow |
| 11 | `unknown_date` | the date is not in `dim_date` | Arrival date is not in dim_date | a date in 2027 |
| 12 | `duplicate` | another row of the same run has the same natural key | Same natural key as another row of the run; the latest row was kept | two rows for one market, commodity, variety, grade and day |

Notes:
- **Natural key** of a price row: market + commodity code + variety + grade + arrival date.
- **Duplicate rule** runs last and compares only rows that passed every other rule. The row with the latest ingest time (then the highest row number) is kept. The others are rejected with status `REJECTED`; all other failed rows have status `ERROR`.
- **Standard names:** the clean table keeps the raw district and adds `district_std`, the standard district name from `dim_market`. A district spelt differently in the file is not a failure.
- **Reference data:** a market or commodity that is not in `seed/` is rejected, not added. To add one, edit the seed CSV and run the dimension loads again, then load the file again.

## Dimension checks (seed files)

Before a dimension table is written, its bronze seed table is checked. If any check fails, the silver table is left unchanged and the task fails with the messages.

| Check | Meaning |
|---|---|
| Castable | every value can be cast to the column type |
| Not null | the key columns are filled |
| Unique | the key is unique |
| No duplicate natural key | market: state + district + market name; commodity: commodity code; date: the date |
| No date gaps | `dim_date` has no missing day between its first and last date |

## Looking at the results

```sql
-- how many rows were rejected, and why
SELECT reject_rule, COUNT(*) AS rows
FROM silver.mandi_prices_rejected
GROUP BY reject_rule
ORDER BY rows DESC;
```

Rejected rows are for review. They are not reprocessed: to correct data, fix the source file and load it again.
