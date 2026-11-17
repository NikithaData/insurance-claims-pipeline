# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup & generate source data
# MAGIC Simulates the legacy claims system extract: generates synthetic customers, policies,
# MAGIC claims and payments and lands them as CSV files in a Unity Catalog volume.
# MAGIC
# MAGIC The data deliberately contains quality issues (duplicates, inconsistent status codes,
# MAGIC missing fields, orphan policies, paid > claimed) for the pipeline to detect.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.text("n_claims", "50000")
N_CLAIMS = int(dbutils.widgets.get("n_claims"))

# COMMAND ----------

import os
import sys

# The generator lives in ../data_generator inside this Git folder
sys.path.append(os.path.abspath("../data_generator"))
import generate_claims  # noqa: E402

generate_claims.main(n_claims=N_CLAIMS, out=RAW_PATH)

# COMMAND ----------

for table in TABLES:
    files = dbutils.fs.ls(f"{RAW_PATH}/{table}")
    print(table, "->", [f.name for f in files])
