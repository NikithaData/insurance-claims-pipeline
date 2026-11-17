# Power BI Dashboard Guide

## 1. Connect Power BI to Databricks
1. In Databricks, open **SQL Warehouses** → your warehouse → **Connection details**. Copy the **Server hostname** and **HTTP path**.
2. Create a personal access token: **Settings → Developer → Access tokens → Generate new token**.
3. In Power BI Desktop: **Get Data → Azure Databricks** (or **Databricks**), paste hostname + HTTP path, then sign in with **Personal Access Token**.
4. Choose **Import** mode, then select these tables from `workspace.insurance_claims`:
   `fact_claims`, `dim_date`, `dim_customer`, `dim_policy`, `kpi_loss_ratio`, `validation_log`, `silver_claims_quarantine`

## 2. Relationships (star schema)
| From (many) | To (one) |
|---|---|
| fact_claims[claim_date_key] | dim_date[date_key] |
| fact_claims[customer_id] | dim_customer[customer_id] |
| fact_claims[policy_id] | dim_policy[policy_id] |

Mark `dim_date` as a date table (Table tools → Mark as date table → `date`).

## 3. DAX measures
```DAX
Total Claims = COUNTROWS(fact_claims)

Open Claims = CALCULATE([Total Claims], fact_claims[status] = "OPEN")

Closed Claims = CALCULATE([Total Claims], fact_claims[status] = "CLOSED")

Denial Rate % = DIVIDE(CALCULATE([Total Claims], fact_claims[status] = "DENIED"), [Total Claims])

Incurred Amount = SUM(fact_claims[claim_amount])

Paid Amount = SUM(fact_claims[paid_amount])

Avg Claim Size = DIVIDE([Incurred Amount], [Total Claims])

Avg Cycle Time (Days) = AVERAGE(fact_claims[cycle_time_days])

Flagged Claims = CALCULATE([Total Claims], fact_claims[is_flagged] = TRUE())

Flagged % = DIVIDE([Flagged Claims], [Total Claims])

Loss Ratio % = DIVIDE(SUM(kpi_loss_ratio[paid_losses]), SUM(kpi_loss_ratio[earned_premium]))

Claims MoM % =
VAR Curr = [Total Claims]
VAR Prev = CALCULATE([Total Claims], DATEADD(dim_date[date], -1, MONTH))
RETURN DIVIDE(Curr - Prev, Prev)

Latest Validation Run = CALCULATE(MAX(validation_log[run_ts]), ALL(validation_log))

Checks Passed (Latest Run) =
VAR LatestRun = [Latest Validation Run]
RETURN CALCULATE(COUNTROWS(validation_log), validation_log[status] = "PASS", validation_log[run_ts] = LatestRun)

Checks Failed (Latest Run) =
VAR LatestRun = [Latest Validation Run]
RETURN CALCULATE(COUNTROWS(validation_log), validation_log[status] = "FAIL", validation_log[run_ts] = LatestRun)
```

## 4. Suggested report pages
**Page 1 – Claims Overview**
- KPI cards: Total Claims, Open Claims, Incurred Amount, Paid Amount, Avg Cycle Time, Loss Ratio %
- Line chart: Total Claims by `dim_date[year_month]`
- Bar chart: Incurred vs Paid by `product_line`
- Map / filled map: claims by `dim_customer[state]`
- Slicers: product_line, claim_type, year

**Page 2 – Risk & Fraud Flags**
- Cards: Flagged Claims, Flagged %
- Stacked bar: flagged claims by flag type (early claim / high amount / late report)
- Table: top 20 flagged claims by claim_amount

**Page 3 – Data Quality**
- Cards: Checks Passed, Checks Failed, Latest Validation Run
- Table: `validation_log` filtered to the latest run (check_name, layer, expected, actual, status) with conditional formatting on status
- Bar chart: `silver_claims_quarantine` count by `dq_reason`

Save as `dashboard/claims_dashboard.pbix` and export screenshots to `dashboard/screenshots/` for the README and LinkedIn post.

## No Power BI? Use a Databricks dashboard
In Databricks: **Dashboards → Create dashboard**, add datasets with SQL like:
```sql
SELECT product_line, COUNT(*) AS claims, SUM(claim_amount) AS incurred, SUM(paid_amount) AS paid
FROM workspace.insurance_claims.fact_claims
GROUP BY product_line;
```
