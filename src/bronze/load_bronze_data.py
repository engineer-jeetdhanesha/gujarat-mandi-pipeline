# Databricks notebook source
# MAGIC %md
# MAGIC # Load bronze: mandi_raw (API or file)
# MAGIC
# MAGIC Loads Gujarat mandi prices into `bronze.mandi_raw` exactly as received (all STRING),
# MAGIC plus ingestion metadata columns. `mode` picks the source:
# MAGIC - `api`: data.gov.in API ("Variety-wise Daily Market Prices"), one `arrival_date`
# MAGIC - `file`: every `mandi_*.csv` in the `bronze.raw_files` volume, loaded whole (`arrival_date` is not used)
# MAGIC
# MAGIC Triggered by `build` with `dbutils.notebook.run`; can also be run on its own.
# MAGIC
# MAGIC Append only: re-running the same date adds duplicate rows; they are removed in silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalog", "gujarat_mandi_pipeline_dev_ws", "Unity Catalog name")
dbutils.widgets.text("arrival_date", "", "Arrival date (yyyy-MM-dd), api mode only")
dbutils.widgets.text("run_id", "", "Job run ID ({{job.run_id}})")
dbutils.widgets.text("mode", "api", "Load mode (api / file)")

catalog = dbutils.widgets.get("catalog")
run_id = dbutils.widgets.get("run_id").strip() or None

mode = dbutils.widgets.get("mode").strip().lower()
if mode not in ("api", "file"):
    raise ValueError(f"mode must be 'api' or 'file', got '{mode}'")

# arrival_date is only needed (and validated) in api mode.
arrival_date = None
if mode == "api":
    arrival_date_param = dbutils.widgets.get("arrival_date").strip()
    try:
        arrival_date = datetime.strptime(arrival_date_param, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"arrival_date must be yyyy-MM-dd, got '{arrival_date_param}'")

print(f"Parameters: catalog={catalog}, mode={mode}, arrival_date={arrival_date}, run_id={run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

SECRET_SCOPE = "secret-scope-gujarat-mandi-pipeline"
SECRET_KEY = "data_gov_api_key"

API_URL = "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24"
STATE = "Gujarat"
PAGE_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = 60

# File mode: CSV in the bronze.raw_files volume (created by setup_bronze).
FILE_VOLUME_DIR = "/Volumes/{catalog}/bronze/raw_files"
FILE_NAME_PATTERN = "mandi_*.csv"

# Source columns in table order; the API returns the same field names
# and the CSV file has them as its header.
SOURCE_COLUMNS = [
    "State",
    "District",
    "Market",
    "Commodity",
    "Commodity_Code",
    "Variety",
    "Grade",
    "Arrival_Date",
    "Min_Price",
    "Max_Price",
    "Modal_Price",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch from API

# COMMAND ----------

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def fetch__api__mandi_records(api_key: str, arrival_date) -> list[dict]:
    """Fetch all Gujarat mandi price records for one arrival date from data.gov.in.

    Pages through the API (limit/offset) until a page returns fewer than
    PAGE_SIZE records. Retries connection errors and 5xx responses.

    Args:
        api_key: data.gov.in API key.
        arrival_date: Date to fetch (datetime.date).

    Returns:
        List of records as returned by the API (dicts keyed by field name).
    """
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))

    print(f"API: fetching {STATE} records for {arrival_date:%d/%m/%Y} (page size {PAGE_SIZE}) ...")
    records = []
    offset = 0
    while True:
        response = session.get(
            API_URL,
            params={
                "api-key": api_key,
                "format": "json",
                "filters[State]": STATE,
                "filters[Arrival_Date]": arrival_date.strftime("%d/%m/%Y"),
                "limit": PAGE_SIZE,
                "offset": offset,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            # Don't include the URL: it contains the API key.
            raise RuntimeError(f"data.gov.in API returned HTTP {response.status_code} at offset {offset}")

        page = response.json().get("records", [])
        print(f"API: offset {offset}: {len(page)} records")
        records.extend(page)
        if len(page) < PAGE_SIZE:
            print(f"API: done, {len(records)} records in total")
            return records
        offset += PAGE_SIZE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load bronze.mandi_raw (API)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def load__bronze__mandi_raw__with_api(catalog: str, arrival_date, api_key: str, run_id: str | None) -> int:
    """Load one arrival date from the data.gov.in API into bronze.mandi_raw.

    Values are stored as received (STRING). Adds _source = 'api',
    _source_file = NULL, _ingested_at and _run_id. Append only.
    Writes nothing if the API returns no records (e.g. mandi holiday).

    Args:
        catalog: Unity Catalog name, e.g. "gujarat_mandi_pipeline_dev_ws".
        arrival_date: Date to load (datetime.date).
        api_key: data.gov.in API key.
        run_id: Databricks job run ID, or None for interactive runs.

    Returns:
        Number of rows written.
    """
    print("Step 1/3: fetching from API")
    records = fetch__api__mandi_records(api_key, arrival_date)
    if not records:
        print("No records returned (mandi holiday?). Nothing written.")
        return 0

    print(f"Step 2/3: building DataFrame from {len(records)} records")
    schema = StructType([StructField(c, StringType()) for c in SOURCE_COLUMNS])
    rows = [
        tuple(None if r.get(c) is None else str(r.get(c)) for c in SOURCE_COLUMNS)
        for r in records
    ]

    df = (
        spark.createDataFrame(rows, schema)
        .withColumn("_source", F.lit("api"))
        .withColumn("_source_file", F.lit(None).cast("string"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id).cast("string"))
    )
    print(f"Step 3/3: appending to {catalog}.bronze.mandi_raw")
    df.write.mode("append").saveAsTable(f"{catalog}.bronze.mandi_raw")
    print(f"Wrote {len(rows)} rows")
    return len(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load bronze.mandi_raw (file)

# COMMAND ----------

def load__bronze__mandi_raw__with_file(catalog: str, run_id: str | None) -> int:
    """Load all mandi_*.csv files from the bronze volume into bronze.mandi_raw.

    Every matching file is loaded whole, with no date filter; the arrival date
    comes from the Arrival_Date column. Values are stored as received (STRING).
    Adds _source = 'file', _source_file = path of the row's file, _ingested_at
    and _run_id. Append only. Writes nothing if the files have no rows.

    Args:
        catalog: Unity Catalog name, e.g. "gujarat_mandi_pipeline_dev_ws".
        run_id: Databricks job run ID, or None for interactive runs.

    Returns:
        Number of rows written.
    """
    path = FILE_VOLUME_DIR.format(catalog=catalog) + "/" + FILE_NAME_PATTERN

    print(f"Step 1/3: reading CSV files from {path}")
    schema = StructType([StructField(c, StringType()) for c in SOURCE_COLUMNS])
    # Raises if no file matches the pattern.
    df = spark.read.option("header", True).schema(schema).csv(path)
    df = (
        df.withColumn("_source", F.lit("file"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id).cast("string"))
    )
    row_count = df.count()
    source_files = sorted(r["_source_file"] for r in df.select("_source_file").distinct().collect())
    print(f"Step 2/3: found {row_count} rows in {len(source_files)} file(s)")
    for source_file in source_files:
        print(f"  {source_file}")
    if row_count == 0:
        print("Files are empty. Nothing written.")
        return 0

    print(f"Step 3/3: appending to {catalog}.bronze.mandi_raw")
    df.write.mode("append").saveAsTable(f"{catalog}.bronze.mandi_raw")
    print(f"Wrote {row_count} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

print(f"Starting bronze.mandi_raw load, mode={mode}")
if mode == "api":
    print(f"Reading API key from secret scope '{SECRET_SCOPE}' (value not printed)")
    api_key = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
    row_count = load__bronze__mandi_raw__with_api(catalog, arrival_date, api_key, run_id)
else:
    row_count = load__bronze__mandi_raw__with_file(catalog, run_id)
print(f"Rows loaded into {catalog}.bronze.mandi_raw (mode={mode}, arrival_date={arrival_date}): {row_count}")
print("Load finished")

dbutils.notebook.exit(str(row_count))
