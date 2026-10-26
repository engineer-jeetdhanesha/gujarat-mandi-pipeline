"""Spark read options of the files the pipeline reads."""

# Source file: fail on any row that does not fit the schema.
OPTIONS_READ_SOURCE = {
    "header": True,
    "mode": "FAILFAST",
    "quote": '"',
    "escape": '"',
}

# Seed files: the header names must match the schema, and a bad row fails the read.
OPTIONS_READ_SEED = {
    "header": True,
    "enforceSchema": False,
    "mode": "FAILFAST",
    "encoding": "UTF-8",
    "quote": '"',
    "escape": '"',
}
