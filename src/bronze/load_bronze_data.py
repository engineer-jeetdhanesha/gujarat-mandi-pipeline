# Databricks notebook source
# MAGIC %md
# MAGIC # Load bronze: mandi_raw (API)
# MAGIC
# MAGIC Functions to fetch Gujarat mandi prices for one arrival date from the data.gov.in API
# MAGIC ("Variety-wise Daily Market Prices") and append them to `bronze.mandi_raw`
# MAGIC exactly as received (all STRING), plus ingestion metadata columns.
# MAGIC
# MAGIC Not run directly: `build` loads these functions with `%run` and calls them.
# MAGIC
# MAGIC Append only: re-running the same date adds duplicate rows; they are removed in silver.

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

# Source columns in table order; the API returns the same field names.
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
        records.extend(page)
        if len(page) < PAGE_SIZE:
            return records
        offset += PAGE_SIZE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load bronze.mandi_raw

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
    records = fetch__api__mandi_records(api_key, arrival_date)
    if not records:
        return 0

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
    df.write.mode("append").saveAsTable(f"{catalog}.bronze.mandi_raw")
    return len(rows)
