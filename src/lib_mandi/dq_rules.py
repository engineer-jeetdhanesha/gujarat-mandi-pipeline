"""Data quality rules for the silver load.

Each rule is (rule id, reason, SQL condition that is true when the row FAILS the rule).
The order is the order in which failed rule ids are listed in reject_rule.

Columns the conditions can use:
- t_<column>: source value with leading and trailing whitespace removed
- d_date, d_min, d_max, d_modal, d_code: typed values, NULL when blank or not valid
- dm_market_key, dc_commodity_key, dd_date_key: dimension lookups, NULL when not found

A rule that needs a valid value is skipped when that value is missing or invalid,
so one bad value gives one reason.
"""

DQ_RULES = [
    # Market name is blank.
    ("missing_market", "Market is missing",
     "t_market IS NULL OR t_market = ''"),

    # Commodity code is blank.
    ("missing_commodity_code", "Commodity code is missing",
     "t_commodity_code IS NULL OR t_commodity_code = ''"),

    # Arrival date is blank or not dd/MM/yyyy.
    ("invalid_date", "Arrival date is missing or not dd/MM/yyyy",
     "d_date IS NULL"),

    # A price is blank or not a number.
    ("invalid_price", "A price is missing or not a number",
     "d_min IS NULL OR d_max IS NULL OR d_modal IS NULL"),

    # A price is zero or negative (checked only when all three prices are valid).
    ("price_le_zero", "A price is zero or negative",
     "d_min IS NOT NULL AND d_max IS NOT NULL AND d_modal IS NOT NULL AND least(d_min, d_max, d_modal) <= 0"),

    # Min price is above max price (checked only when all three prices are valid).
    ("min_gt_max", "Min price is greater than max price",
     "d_min IS NOT NULL AND d_max IS NOT NULL AND d_modal IS NOT NULL AND d_min > d_max"),

    # Modal price is below min or above max (checked only when all prices are valid and min is not above max).
    ("modal_out_of_range", "Modal price is outside the min-max range",
     "d_min IS NOT NULL AND d_max IS NOT NULL AND d_modal IS NOT NULL AND d_min <= d_max "
     "AND (d_modal < d_min OR d_modal > d_max)"),

    # Market is filled but not in silver.dim_market.
    ("unknown_market", "Market is not in dim_market",
     "t_market IS NOT NULL AND t_market <> '' AND dm_market_key IS NULL"),

    # Commodity code is filled but not in silver.dim_commodity.
    ("unknown_commodity", "Commodity code is not in dim_commodity",
     "t_commodity_code IS NOT NULL AND t_commodity_code <> '' AND dc_commodity_key IS NULL"),

    # Arrival date is after today.
    ("future_date", "Arrival date is in the future",
     "d_date IS NOT NULL AND d_date > current_date()"),

    # Arrival date is valid but not in silver.dim_date.
    ("unknown_date", "Arrival date is not in dim_date",
     "d_date IS NOT NULL AND dd_date_key IS NULL"),
]

# Not in the list above: the duplicate rule (it needs the other rows). Among rows that pass every
# rule, rows with the same natural key (market, commodity code, variety, grade, arrival date) are
# duplicates; the latest _ingested_at, then the highest _row_no, wins and the others are rejected
# with rule id "duplicate".
