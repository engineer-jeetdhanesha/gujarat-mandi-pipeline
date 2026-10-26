# Databricks notebook source
# MAGIC %md
# MAGIC # Seed: silver dimension tables
# MAGIC
# MAGIC Loads the CSV files in `seed/` into `silver.dim_market`, `silver.dim_commodity` and `silver.dim_date`
# MAGIC in the workspace's default catalog (the tables are created by `src/setup`).
# MAGIC
# MAGIC - CSV files only. The column names must match the table's columns (`updated_at` excluded), in the same order.
# MAGIC - Values are read with the schemas in `lib_mandi.schemas`; `updated_at` is set to the load time.
# MAGIC - INSERT OVERWRITE: each table is replaced by the file's rows, so the CSV is the source of truth.
# MAGIC   Add a dimension row by editing the CSV and running this notebook again.
# MAGIC - An empty file is refused, so a bad file cannot wipe a table.
# MAGIC
# MAGIC Parameters:
# MAGIC - `seed_dir` (optional): folder holding the CSVs. Default: the `seed` folder next to `src`.
# MAGIC
# MAGIC If a table fails to load, the reason is printed and the notebook still completes.

# COMMAND ----------

dbutils.widgets.text("seed_dir", "", "Seed folder (optional)")

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def get__seed_dir(seed_dir_param: str) -> str:
    """Return the folder holding the seed CSVs.

    Args:
        seed_dir_param: Value of the seed_dir widget; empty to use the default.

    Returns:
        The given folder, or the "seed" folder next to "src" in the workspace
        (this notebook is src/load_silver_dims); empty string if it could not be worked out
        (the reason is printed).
    """
    import os

    if seed_dir_param:
        return seed_dir_param
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        repo_root = os.path.dirname(os.path.dirname(notebook_path))
        return f"/Workspace{repo_root}/seed"
    except Exception as e:
        print(f"ERROR: could not work out the seed folder, pass the seed_dir widget: {e}")
        return ""

# COMMAND ----------

def load__silver_dims(
    spark: "SparkSession",
    file_path: str,
    schema: "StructType",
    options: dict[str, object],
    table_name: str,
) -> bool:
    """Replace a dimension table with the rows of a seed CSV (INSERT OVERWRITE).

    The file needs a header row whose names equal the schema's columns, in the same order.
    Values are read with the schema's types and updated_at is set to the load time.

    Args:
        spark: Spark session.
        file_path: Path of the CSV file, e.g. "/Workspace/Users/me/repo/seed/dim_market.csv".
        schema: Schema of the file, i.e. the table's columns without updated_at.
        options: Spark read options (lib_mandi.config.OPTIONS_READ_SEED).
        table_name: Three-level table name, e.g. "mandi_dev.silver.dim_market".

    Returns:
        True if the table was loaded; False if an input is invalid, the file is empty
        or the load failed (the reason is printed). The table is unchanged on False.
    """
    from pyspark.sql import functions as F

    if not file_path.lower().endswith(".csv"):
        print(f"ERROR: {file_path} is not a .csv file")
        return False

    try:
        read_path = f"file:{file_path}" if file_path.startswith("/Workspace") else file_path
        print(f"Reading {file_path} ...")
        df = spark.read.schema(schema).options(**options).csv(read_path)

        row_count = df.count()
        if row_count == 0:
            print(f"ERROR: no data rows in {file_path}; {table_name} left unchanged")
            return False

        df = df.withColumn("updated_at", F.current_timestamp())

        print(f"Overwriting {table_name} with {row_count} rows ...")
        df.write.insertInto(table_name, overwrite=True)
        print(f"Loaded {row_count} rows into {table_name}")
        return True
    except Exception as e:
        print(f"ERROR: seed load failed for {table_name}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

from lib_mandi.config import OPTIONS_READ_SEED
from lib_mandi.schemas import SCHEMA_DIM_COMMODITY, SCHEMA_DIM_DATE, SCHEMA_DIM_MARKET

catalog = get__default_catalog()
print(f"Seeding silver dimensions in catalog: {catalog}")

if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

seed_dir = get__seed_dir(dbutils.widgets.get("seed_dir").strip())
if not seed_dir:
    raise RuntimeError("Could not work out the seed folder (see the error printed above)")
print(f"Seed folder: {seed_dir}")

seeds = [
    ("dim_market.csv", f"{catalog}.silver.dim_market", SCHEMA_DIM_MARKET),
    ("dim_commodity.csv", f"{catalog}.silver.dim_commodity", SCHEMA_DIM_COMMODITY),
    ("dim_date.csv", f"{catalog}.silver.dim_date", SCHEMA_DIM_DATE),
]

for file_name, table_name, schema in seeds:
    load__silver_dims(spark, f"{seed_dir}/{file_name}", schema, OPTIONS_READ_SEED, table_name)
