# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — star schema & KPI tables
# MAGIC - `dim_date`, `dim_customer`, `dim_policy`
# MAGIC - `fact_claims` with cycle time, report lag, payments and fraud-risk flags
# MAGIC - `kpi_claims_monthly`, `kpi_loss_ratio` for reporting

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def save(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(name))
    print(f"{name}: {spark.table(t(name)).count():,} rows")

# COMMAND ----------

# MAGIC %md ### Dimensions

# COMMAND ----------

dim_date = (
    spark.sql("SELECT explode(sequence(to_date('2020-01-01'), to_date('2027-12-31'), interval 1 day)) AS date")
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.date_format("date", "E").alias("day_of_week"),
    )
)
save(dim_date, "dim_date")

age = F.floor(F.months_between(F.current_date(), F.col("dob")) / 12)
dim_customer = spark.table(t("silver_customers")).select(
    "customer_id",
    F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
    "state",
    age.alias("age"),
    F.when(age < 25, "18-24").when(age < 35, "25-34").when(age < 45, "35-44")
     .when(age < 55, "45-54").when(age < 65, "55-64").otherwise("65+").alias("age_band"),
    F.floor(F.months_between(F.current_date(), F.col("customer_since")) / 12).alias("tenure_years"),
)
save(dim_customer, "dim_customer")

dim_policy = spark.table(t("silver_policies")).select(
    "policy_id", "customer_id", "product_line", "coverage_type",
    "annual_premium", "start_date", "end_date",
)
save(dim_policy, "dim_policy")

# COMMAND ----------

# MAGIC %md ### Fact table

# COMMAND ----------

pay_totals = spark.table(t("silver_payments")).groupBy("claim_id").agg(
    F.sum("amount").alias("total_payments"),
    F.count("*").alias("payment_count"),
    F.max("payment_date").alias("last_payment_date"),
)

claims = (
    spark.table(t("silver_claims"))
    .join(dim_policy.select("policy_id", "product_line", F.col("start_date").alias("policy_start")), "policy_id", "left")
    .join(pay_totals, "claim_id", "left")
)

avg_by_line = Window.partitionBy("product_line")
early_claim = F.datediff("claim_date", "policy_start") <= 30
high_amount = F.col("claim_amount") > 3 * F.avg("claim_amount").over(avg_by_line)
late_report = F.datediff("report_date", "claim_date") > 21

fact_claims = claims.select(
    "claim_id", "policy_id", "customer_id", "product_line", "claim_type", "status",
    F.date_format("claim_date", "yyyyMMdd").cast("int").alias("claim_date_key"),
    "claim_date", "report_date", "close_date",
    "claim_amount", "paid_amount",
    F.coalesce("total_payments", F.lit(0).cast("decimal(14,2)")).alias("total_payments"),
    F.coalesce("payment_count", F.lit(0)).alias("payment_count"),
    F.datediff("report_date", "claim_date").alias("report_lag_days"),
    F.datediff("close_date", "report_date").alias("cycle_time_days"),
    early_claim.alias("flag_early_claim"),
    high_amount.alias("flag_high_amount"),
    late_report.alias("flag_late_report"),
    (early_claim | high_amount | late_report).alias("is_flagged"),
)
save(fact_claims, "fact_claims")

# COMMAND ----------

# MAGIC %md ### KPI tables

# COMMAND ----------

fact = spark.table(t("fact_claims"))

kpi_monthly = (
    fact.withColumn("year_month", F.date_format("claim_date", "yyyy-MM"))
    .groupBy("year_month", "product_line")
    .agg(
        F.count("*").alias("claim_count"),
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_claims"),
        F.sum("claim_amount").alias("incurred_amount"),
        F.sum("paid_amount").alias("paid_amount"),
        F.round(F.avg("cycle_time_days"), 1).alias("avg_cycle_time_days"),
        F.sum(F.col("is_flagged").cast("int")).alias("flagged_claims"),
    )
    .orderBy("year_month", "product_line")
)
save(kpi_monthly, "kpi_claims_monthly")

premiums = spark.table(t("dim_policy")).groupBy("product_line").agg(F.sum("annual_premium").alias("earned_premium"))
paid = fact.groupBy("product_line").agg(F.sum("paid_amount").alias("paid_losses"))
kpi_loss_ratio = premiums.join(paid, "product_line", "left").withColumn(
    "loss_ratio_pct", F.round(F.col("paid_losses") / F.col("earned_premium") * 100, 1)
)
save(kpi_loss_ratio, "kpi_loss_ratio")
display(kpi_loss_ratio)
