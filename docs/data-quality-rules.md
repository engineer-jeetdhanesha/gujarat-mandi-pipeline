# Data quality rules

| ID | Rule | What it does |
|---|---|---|
| 1 | `missing_market` | Rejects a row with a blank market |
| 2 | `missing_commodity_code` | Rejects a row with a blank commodity code |
| 3 | `invalid_date` | Rejects a row whose date is blank or not in `dd/MM/yyyy` format |
| 4 | `invalid_price` | Rejects a row whose min, max or modal price is blank or not a number |
| 5 | `price_le_zero` | Rejects a row with a price of zero or less |
| 6 | `min_gt_max` | Rejects a row whose min price is above its max price |
| 7 | `modal_out_of_range` | Rejects a row whose modal price is below the min or above the max price |
| 8 | `unknown_market` | Rejects a row whose market is not in `dim_market` |
| 9 | `unknown_commodity` | Rejects a row whose commodity code is not in `dim_commodity` |
| 10 | `future_date` | Rejects a row dated after today |
| 11 | `unknown_date` | Rejects a row whose date is not in `dim_date` |
| 12 | `duplicate` | Rejects extra rows with the same market, commodity code, variety, grade and date in a run, and keeps the latest one |
