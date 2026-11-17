# Databricks notebook source
# MAGIC %md
# MAGIC # Shared configuration
# MAGIC Included by every pipeline notebook with `%run ./_config`.
# MAGIC Change `catalog` / `schema` with the widgets at the top of the notebook (or as job parameters).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")          # Free Edition default catalog
dbutils.widgets.text("schema", "insurance_claims")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = "raw"

RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/landing"         # landing zone (one folder per table)
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/_checkpoints"
TABLES = ["customers", "policies", "claims", "payments"]


def t(name: str) -> str:
    """Fully qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Catalog/schema : {CATALOG}.{SCHEMA}")
print(f"Landing path   : {RAW_PATH}")
