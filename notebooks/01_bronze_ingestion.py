# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — raw ingestion with Auto Loader
# MAGIC Incrementally ingests every CSV that lands in the volume into Delta tables **as-is**
# MAGIC (all columns kept as strings), plus audit columns.
# MAGIC Auto Loader tracks processed files in a checkpoint, so re-running only picks up new files.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F


def ingest_to_bronze(table: str) -> None:
    source = f"{RAW_PATH}/{table}"
    target = t(f"bronze_{table}")
    chk = f"{CHECKPOINT_PATH}/bronze_{table}"

    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")   # keep raw values as strings in bronze
        .option("cloudFiles.schemaLocation", f"{chk}/schema")
        .load(source)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", f"{chk}/checkpoint")
        .trigger(availableNow=True)                         # process what's there, then stop
        .toTable(target)
    )
    query.awaitTermination()
    print(f"{target}: {spark.table(target).count():,} rows")


for table in TABLES:
    ingest_to_bronze(table)
