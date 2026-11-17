# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — clean, standardize, deduplicate, quarantine
# MAGIC - Casts raw strings to proper types
# MAGIC - Standardizes claim status codes (`OPN`, `Open`, `O` → `OPEN`)
# MAGIC - Removes duplicate claims, keeping the latest `last_updated` version
# MAGIC - Sends records that break data-quality rules to `silver_claims_quarantine` with a reason
# MAGIC - Upserts clean claims into `silver_claims` with `MERGE` (incremental-safe)

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def latest_by_key(df, key, order_col):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# COMMAND ----------

# MAGIC %md ### Customers, policies, payments

# COMMAND ----------

customers = latest_by_key(
    spark.table(t("bronze_customers")).select(
        F.trim("customer_id").alias("customer_id"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.to_date("dob").alias("dob"),
        F.upper(F.trim("state")).alias("state"),
        F.to_date("customer_since").alias("customer_since"),
        "_ingest_ts",
    ),
    "customer_id", "_ingest_ts",
)

policies = latest_by_key(
    spark.table(t("bronze_policies")).select(
        F.trim("policy_id").alias("policy_id"),
        F.trim("customer_id").alias("customer_id"),
        F.upper(F.trim("product_line")).alias("product_line"),
        F.trim("coverage_type").alias("coverage_type"),
        F.col("annual_premium").cast("decimal(14,2)").alias("annual_premium"),
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        "_ingest_ts",
    ),
    "policy_id", "_ingest_ts",
)

payments = latest_by_key(
    spark.table(t("bronze_payments")).select(
        F.trim("payment_id").alias("payment_id"),
        F.trim("claim_id").alias("claim_id"),
        F.to_date("payment_date").alias("payment_date"),
        F.col("amount").cast("decimal(14,2)").alias("amount"),
        F.upper(F.trim("payment_method")).alias("payment_method"),
        "_ingest_ts",
    ),
    "payment_id", "_ingest_ts",
)

for name, df in [("silver_customers", customers), ("silver_policies", policies), ("silver_payments", payments)]:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(name))
    print(f"{name}: {spark.table(t(name)).count():,} rows")

# COMMAND ----------

# MAGIC %md ### Claims

# COMMAND ----------

STATUS_MAP = {
    "OPEN": "OPEN", "OPN": "OPEN", "O": "OPEN",
    "CLOSED": "CLOSED", "CLS": "CLOSED", "C": "CLOSED",
    "DENIED": "DENIED", "DEN": "DENIED", "D": "DENIED",
    "IN_REVIEW": "IN_REVIEW", "IN REVIEW": "IN_REVIEW", "REV": "IN_REVIEW",
}
status_map = F.create_map(*[F.lit(x) for kv in STATUS_MAP.items() for x in kv])

bronze_claims = spark.table(t("bronze_claims"))

typed = bronze_claims.select(
    F.trim("claim_id").alias("claim_id"),
    F.trim("policy_id").alias("policy_id"),
    F.trim("customer_id").alias("customer_id"),
    F.to_date("claim_date").alias("claim_date"),
    F.to_date("report_date").alias("report_date"),
    F.to_date("close_date").alias("close_date"),
    F.col("claim_amount").cast("decimal(14,2)").alias("claim_amount"),
    F.col("paid_amount").cast("decimal(14,2)").alias("paid_amount"),
    F.coalesce(status_map[F.upper(F.trim("status"))], F.lit("UNKNOWN")).alias("status"),
    F.col("status").alias("status_raw"),
    F.trim("claim_type").alias("claim_type"),
    F.to_timestamp("last_updated").alias("last_updated"),
    "_ingest_ts",
)

# 1) Deduplicate: keep the most recent version of each claim
w = Window.partitionBy("claim_id").orderBy(F.col("last_updated").desc(), F.col("_ingest_ts").desc())
deduped = typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# 2) Data-quality rules -> reason text ("" when the record is clean)
valid_policies = spark.table(t("silver_policies")).select("policy_id").withColumn("_policy_exists", F.lit(True))
checked = deduped.join(valid_policies, on="policy_id", how="left").withColumn(
    "dq_reason",
    F.concat_ws(
        "; ",
        F.when(F.col("policy_id").isNull(), F.lit("missing policy_id")),
        F.when(F.col("claim_date").isNull(), F.lit("missing claim_date")),
        F.when(F.col("policy_id").isNotNull() & F.col("_policy_exists").isNull(), F.lit("orphan policy_id")),
        F.when(F.col("paid_amount") > F.col("claim_amount"), F.lit("paid exceeds claimed")),
        F.when(F.col("close_date") < F.col("report_date"), F.lit("close date before report date")),
        F.when(F.col("status") == "UNKNOWN", F.lit("unknown status code")),
    ),
).drop("_policy_exists")

clean = checked.filter(F.col("dq_reason") == "").drop("dq_reason")
quarantine = checked.filter(F.col("dq_reason") != "").withColumn("quarantined_ts", F.current_timestamp())

# COMMAND ----------

# 3) Quarantine table (rebuilt each run)
quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_claims_quarantine"))

# 4) Upsert clean claims with MERGE so re-runs and late-arriving updates are handled
if not spark.catalog.tableExists(t("silver_claims")):
    clean.write.saveAsTable(t("silver_claims"))
else:
    clean.createOrReplaceTempView("claims_updates")
    spark.sql(f"""
        MERGE INTO {t('silver_claims')} AS tgt
        USING claims_updates AS src
          ON tgt.claim_id = src.claim_id
        WHEN MATCHED AND src.last_updated > tgt.last_updated THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

bronze_n = bronze_claims.count()
dedup_n = deduped.count()
print(f"bronze_claims rows        : {bronze_n:,}")
print(f"duplicates removed        : {bronze_n - dedup_n:,}")
print(f"silver_claims rows        : {spark.table(t('silver_claims')).count():,}")
print(f"quarantined rows          : {spark.table(t('silver_claims_quarantine')).count():,}")

display(
    spark.table(t("silver_claims_quarantine"))
    .groupBy("dq_reason").count().orderBy(F.desc("count"))
)
