# Databricks notebook source
# MAGIC %md
# MAGIC # Seed: silver dimension tables
# MAGIC
# MAGIC Loads the CSV files in `seed/` into `silver.dim_market`, `silver.dim_commodity` and `silver.dim_date`
# MAGIC in the workspace's default catalog (the tables are created by `src/setup`).
# MAGIC
# MAGIC - CSV files only. The column names must match the table's columns (`updated_at` excluded), in the same order.
# MAGIC - Values are cast to the table's column types; `updated_at` is set to the load time.
# MAGIC - INSERT OVERWRITE: each table is replaced by the file's rows, so the CSV is the source of truth.
# MAGIC   Add a dimension row by editing the CSV and running this notebook again.
# MAGIC - An empty file is refused, so a bad file cannot wipe a table.
# MAGIC
# MAGIC Parameters:
# MAGIC - `seed_dir` (optional): folder holding the CSVs. Default: the `seed` folder next to `src`.
# MAGIC
# MAGIC Raises an error if any table fails to load, so the run shows as failed.

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
        (this notebook is src/seed_dims); empty string if it could not be worked out
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

def load__table__seed_dim(file_path: str, table_name: str) -> bool:
    """Replace a dimension table with the rows of a seed CSV (INSERT OVERWRITE).

    The file needs a header row whose names equal the table's columns, excluding
    updated_at, in the same order. Values are cast to the table's column types and
    updated_at is set to the load time.

    Args:
        file_path: Path of the CSV file, e.g. "/Workspace/Users/me/repo/seed/dim_market.csv".
        table_name: Three-level table name, e.g. "mandi_dev.silver.dim_market".

    Returns:
        True if the table was loaded; False if an input is invalid, the file is empty
        or the load failed (the reason is printed). The table is unchanged on False.
    """
    from pyspark.sql import functions as F

    if not file_path.lower().endswith(".csv"):
        print(f"ERROR: {file_path} is not a .csv file")
        return False
    if len(table_name.split(".")) != 3:
        print(f"ERROR: table name {table_name} is not three-level (catalog.schema.table)")
        return False

    try:
        table_schema = spark.table(table_name).schema
        table_fields = [f for f in table_schema.fields if f.name != "updated_at"]
        table_columns = [f.name for f in table_fields]

        read_path = f"file:{file_path}" if file_path.startswith("/Workspace") else file_path
        print(f"Reading {file_path} ...")
        df = (
            spark.read
            .option("header", True)
            .option("mode", "FAILFAST")
            .option("encoding", "UTF-8")
            .option("quote", '"')
            .option("escape", '"')
            .csv(read_path)
        )

        if df.columns != table_columns:
            print(f"ERROR: {file_path} has columns {df.columns}, {table_name} expects {table_columns}")
            return False

        row_count = df.count()
        if row_count == 0:
            print(f"ERROR: no data rows in {file_path}; {table_name} left unchanged")
            return False

        df = df.select(
            *[F.col(f.name).cast(f.dataType).alias(f.name) for f in table_fields],
            F.current_timestamp().alias("updated_at"),
        )

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

catalog = get__default_catalog()
print(f"Seeding silver dimensions in catalog: {catalog}")

if not catalog:
    raise RuntimeError("Could not read the default catalog (see the error printed above)")

seed_dir = get__seed_dir(dbutils.widgets.get("seed_dir").strip())
if not seed_dir:
    raise RuntimeError("Could not work out the seed folder (see the error printed above)")
print(f"Seed folder: {seed_dir}")

seeds = [
    ("dim_market.csv", f"{catalog}.silver.dim_market"),
    ("dim_commodity.csv", f"{catalog}.silver.dim_commodity"),
    ("dim_date.csv", f"{catalog}.silver.dim_date"),
]

failed = [table for file_name, table in seeds if not load__table__seed_dim(f"{seed_dir}/{file_name}", table)]
if failed:
    raise RuntimeError(f"Seed failed for: {', '.join(failed)} (see the errors printed above)")

print("Seed complete")
