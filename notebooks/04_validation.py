# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validation — source-to-target reconciliation
# MAGIC Runs after every load and appends results to `validation_log`.
# MAGIC The notebook **fails** if any check fails, so a scheduled job alerts on bad loads.
# MAGIC
# MAGIC | Check | Layers |
# MAGIC |---|---|
# MAGIC | Row counts | source files → bronze |
# MAGIC | Checksums (SUM of amounts) | source → bronze, silver → gold |
# MAGIC | Dedup reconciliation | bronze distinct claims = silver + quarantine |
# MAGIC | Mandatory-field nulls | silver |
# MAGIC | Business rules | silver |
# MAGIC | Referential integrity | gold |
# MAGIC | Payments reconcile to paid amount | silver |

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc)
results = []


def check(name, layer, expected, actual, tolerance=0.0, info_only=False):
    expected_f, actual_f = float(expected or 0), float(actual or 0)
    if info_only:
        status = "INFO"
    else:
        status = "PASS" if abs(expected_f - actual_f) <= tolerance else "FAIL"
    results.append((RUN_ID, RUN_TS, name, layer, expected_f, actual_f, status))


def raw(table):
    return spark.read.option("header", "true").csv(f"{RAW_PATH}/{table}")


def total(df, col):
    return df.agg(F.sum(F.col(col).cast("decimal(18,2)"))).first()[0] or 0

# COMMAND ----------

# 1) Row counts: source files -> bronze
for table in TABLES:
    check(f"row_count_{table}", "source→bronze", raw(table).count(), spark.table(t(f"bronze_{table}")).count())

# 2) Checksums: source -> bronze
check("checksum_claim_amount", "source→bronze", total(raw("claims"), "claim_amount"),
      total(spark.table(t("bronze_claims")), "claim_amount"), tolerance=0.01)
check("checksum_payment_amount", "source→bronze", total(raw("payments"), "amount"),
      total(spark.table(t("bronze_payments")), "amount"), tolerance=0.01)

# 3) Dedup reconciliation: every distinct claim lands in silver OR quarantine
bronze_claims = spark.table(t("bronze_claims"))
silver_claims = spark.table(t("silver_claims"))
quarantine = spark.table(t("silver_claims_quarantine"))
check("dedup_reconciliation", "bronze→silver",
      bronze_claims.select("claim_id").distinct().count(),
      silver_claims.count() + quarantine.count())
check("duplicates_removed", "bronze→silver", 0,
      bronze_claims.count() - bronze_claims.select("claim_id").distinct().count(), info_only=True)
check("records_quarantined", "silver", 0, quarantine.count(), info_only=True)

# 4) Mandatory fields
for col in ["claim_id", "policy_id", "customer_id", "claim_date", "claim_amount", "status"]:
    check(f"null_{col}", "silver", 0, silver_claims.filter(F.col(col).isNull()).count())

# 5) Business rules
check("rule_paid_le_claimed", "silver", 0, silver_claims.filter("paid_amount > claim_amount").count())
check("rule_close_after_report", "silver", 0, silver_claims.filter("close_date < report_date").count())
check("rule_valid_status", "silver", 0,
      silver_claims.filter(~F.col("status").isin("OPEN", "CLOSED", "DENIED", "IN_REVIEW")).count())

# 6) Referential integrity (gold)
fact = spark.table(t("fact_claims"))
check("ri_claim_to_policy", "gold", 0,
      fact.join(spark.table(t("dim_policy")), "policy_id", "left_anti").count())
check("ri_claim_to_customer", "gold", 0,
      fact.join(spark.table(t("dim_customer")), "customer_id", "left_anti").count())

# 7) Silver -> gold reconciliation
check("row_count_fact_claims", "silver→gold", silver_claims.count(), fact.count())
check("checksum_paid_amount", "silver→gold", total(silver_claims, "paid_amount"),
      total(fact, "paid_amount"), tolerance=0.01)

# 8) Payments reconcile to paid amount for closed claims
check("payments_match_paid_amount", "silver", 0,
      fact.filter((F.col("status") == "CLOSED") &
                  (F.abs(F.col("total_payments") - F.col("paid_amount")) > 0.05)).count())

# COMMAND ----------

schema = "run_id string, run_ts timestamp, check_name string, layer string, expected double, actual double, status string"
log_df = spark.createDataFrame(results, schema)
log_df.write.mode("append").saveAsTable(t("validation_log"))

display(log_df.orderBy("status", "check_name"))

# COMMAND ----------

failed = [r[2] for r in results if r[6] == "FAIL"]
passed = sum(1 for r in results if r[6] == "PASS")
print(f"Run {RUN_ID}: {passed} passed, {len(failed)} failed")
if failed:
    raise Exception(f"Data validation failed: {failed}")
