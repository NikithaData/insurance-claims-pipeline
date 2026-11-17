# Data Dictionary

## Source / bronze tables
| Table | Key columns |
|---|---|
| customers | customer_id, first_name, last_name, dob, state, customer_since |
| policies | policy_id, customer_id, product_line (AUTO/HOME/HEALTH/LIFE), coverage_type, annual_premium, start_date, end_date |
| claims | claim_id, policy_id, customer_id, claim_date, report_date, close_date, claim_amount, paid_amount, status, claim_type, last_updated |
| payments | payment_id, claim_id, payment_date, amount, payment_method |

Bronze tables add `_ingest_ts` (load time) and `_source_file` (file path), plus Auto Loader's `_rescued_data`.

## Silver tables
| Table | Description |
|---|---|
| silver_customers / silver_policies / silver_payments | Typed, trimmed, deduplicated |
| silver_claims | Typed, status standardized, deduplicated (latest `last_updated` wins), only records that pass all DQ rules; loaded with MERGE |
| silver_claims_quarantine | Records that failed DQ rules, with `dq_reason` |

### Status standardization
| Raw values | Standard |
|---|---|
| OPEN, Open, OPN, O | OPEN |
| CLOSED, Closed, CLS, C | CLOSED |
| DENIED, Denied, DEN, D | DENIED |
| IN_REVIEW, In Review, REV | IN_REVIEW |

### Data-quality rules (→ quarantine)
- Missing policy_id or claim_date
- Orphan policy_id (not in policies)
- paid_amount > claim_amount
- close_date before report_date
- Unknown status code

## Gold tables
| Table | Grain | Notes |
|---|---|---|
| dim_date | 1 row per day | date_key = yyyyMMdd |
| dim_customer | 1 row per customer | age, age_band, tenure_years |
| dim_policy | 1 row per policy | |
| fact_claims | 1 row per claim | cycle_time_days, report_lag_days, total_payments, risk flags |
| kpi_claims_monthly | month × product_line | counts, incurred, paid, avg cycle time, flagged |
| kpi_loss_ratio | product_line | paid_losses / earned_premium |
| validation_log | check × run | appended every run |

### Risk flags in fact_claims
| Flag | Rule |
|---|---|
| flag_early_claim | Claim within 30 days of policy start |
| flag_high_amount | Claim amount > 3× the product-line average |
| flag_late_report | Reported more than 21 days after the loss date |
| is_flagged | Any of the above |
