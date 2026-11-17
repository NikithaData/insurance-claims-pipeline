"""
Synthetic insurance claims data generator.
Creates customers, policies, claims and payments CSVs that match the
pipeline's data model, with deliberate data-quality issues for the
silver layer to clean (duplicates, inconsistent status codes, nulls).

Writes one folder per table (out/claims/claims.csv, ...) so Auto Loader
can ingest each table from its own landing folder.

Usage: python generate_claims.py --claims 50000 --out data
Only uses the Python standard library.
"""
import argparse
import csv
import os
import random
from datetime import date, timedelta


STATES = ["NJ", "NY", "PA", "CT", "MA", "FL", "TX", "CA", "IL", "OH"]
PRODUCT_LINES = {
    "AUTO": ["Liability", "Collision", "Comprehensive"],
    "HOME": ["Dwelling", "Contents", "Liability"],
    "HEALTH": ["Inpatient", "Outpatient", "Pharmacy"],
    "LIFE": ["Term", "Whole"],
}
CLAIM_TYPES = {
    "AUTO": ["Accident", "Theft", "Glass", "Vandalism"],
    "HOME": ["Fire", "Water Damage", "Theft", "Storm"],
    "HEALTH": ["Hospitalization", "Surgery", "Prescription", "Emergency"],
    "LIFE": ["Death Benefit"],
}
# Messy status codes on purpose -> standardize in silver layer
STATUS_VARIANTS = {
    "OPEN": ["OPEN", "Open", "OPN", "O"],
    "CLOSED": ["CLOSED", "Closed", "CLS", "C"],
    "DENIED": ["DENIED", "Denied", "DEN", "D"],
    "IN_REVIEW": ["IN_REVIEW", "In Review", "REV"],
}
FIRST = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
         "Linda", "David", "Elizabeth", "Raj", "Priya", "Wei", "Maria", "Ahmed"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Patel",
        "Miller", "Davis", "Rodriguez", "Shah", "Chen", "Lee", "Khan", "Lopez"]


def rand_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {os.path.basename(path)}: {len(rows):,} rows")


def main(n_claims=50000, out="data", seed=42):
    random.seed(seed)
    os.makedirs(out, exist_ok=True)
    n_customers = max(100, n_claims // 3)
    n_policies = max(150, n_claims // 2)

    # Customers
    customers = []
    for i in range(1, n_customers + 1):
        dob = rand_date(date(1945, 1, 1), date(2004, 12, 31))
        customers.append([
            f"C{i:07d}", random.choice(FIRST), random.choice(LAST), dob.isoformat(),
            random.choice(STATES), rand_date(date(2010, 1, 1), date(2024, 12, 31)).isoformat(),
        ])

    # Policies
    policies = []
    for i in range(1, n_policies + 1):
        line = random.choice(list(PRODUCT_LINES))
        start = rand_date(date(2020, 1, 1), date(2025, 6, 30))
        premium = round(random.uniform(400, 4000) * (3 if line == "LIFE" else 1), 2)
        policies.append([
            f"P{i:07d}", random.choice(customers)[0], line,
            random.choice(PRODUCT_LINES[line]), premium,
            start.isoformat(), (start + timedelta(days=365)).isoformat(),
        ])

    # Claims + payments
    claims, payments = [], []
    pay_id = 1
    for i in range(1, n_claims + 1):
        pol = random.choice(policies)
        line, pol_start = pol[2], date.fromisoformat(pol[5])
        claim_dt = rand_date(pol_start, pol_start + timedelta(days=364))
        report_dt = claim_dt + timedelta(days=random.randint(0, 30))
        status = random.choices(list(STATUS_VARIANTS), weights=[25, 55, 10, 10])[0]
        close_dt = (report_dt + timedelta(days=random.randint(5, 180))
                    if status in ("CLOSED", "DENIED") else None)

        base = {"AUTO": 4000, "HOME": 12000, "HEALTH": 6000, "LIFE": 150000}[line]
        amount = round(random.lognormvariate(0, 0.8) * base, 2)
        paid = round(amount * random.uniform(0.6, 1.0), 2) if status == "CLOSED" else 0.0

        claims.append([
            f"CLM{i:08d}", pol[0], pol[1], claim_dt.isoformat(), report_dt.isoformat(),
            close_dt.isoformat() if close_dt else "", amount, paid,
            random.choice(STATUS_VARIANTS[status]), random.choice(CLAIM_TYPES[line]),
            (report_dt + timedelta(days=random.randint(0, 5))).isoformat() + " 10:00:00",
        ])

        # Split paid amount into 1-3 payments
        if paid > 0:
            parts = random.randint(1, 3)
            remaining = paid
            for p in range(parts):
                amt = remaining if p == parts - 1 else round(remaining * random.uniform(0.3, 0.6), 2)
                remaining = round(remaining - amt, 2)
                payments.append([f"PAY{pay_id:08d}", f"CLM{i:08d}",
                                 (report_dt + timedelta(days=random.randint(3, 90))).isoformat(),
                                 round(amt, 2), random.choice(["ACH", "CHECK", "WIRE"])])
                pay_id += 1

    # --- Inject data quality issues ---
    # 1) ~2% duplicate claims with a later update timestamp
    for row in random.sample(claims, k=max(1, n_claims // 50)):
        dup = row.copy()
        dup[10] = dup[10].replace("10:00:00", "15:30:00")
        claims.append(dup)
    # 2) ~1% missing mandatory fields
    for row in random.sample(claims, k=max(1, n_claims // 100)):
        row[random.choice([1, 3])] = ""
    # 3) ~0.5% business-rule violations (paid > claimed)
    for row in random.sample(claims, k=max(1, n_claims // 200)):
        row[7] = round(row[6] * 1.25, 2)
    # 4) ~0.5% orphan claims (policy doesn't exist)
    for row in random.sample(claims, k=max(1, n_claims // 200)):
        row[1] = "P9999999"
    random.shuffle(claims)

    print(f"Writing to {out}/")
    write_csv(f"{out}/customers/customers.csv",
              ["customer_id", "first_name", "last_name", "dob", "state", "customer_since"], customers)
    write_csv(f"{out}/policies/policies.csv",
              ["policy_id", "customer_id", "product_line", "coverage_type", "annual_premium",
               "start_date", "end_date"], policies)
    write_csv(f"{out}/claims/claims.csv",
              ["claim_id", "policy_id", "customer_id", "claim_date", "report_date", "close_date",
               "claim_amount", "paid_amount", "status", "claim_type", "last_updated"], claims)
    write_csv(f"{out}/payments/payments.csv",
              ["payment_id", "claim_id", "payment_date", "amount", "payment_method"], payments)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--claims", type=int, default=50000)
    ap.add_argument("--out", default="data")
    args = ap.parse_args()
    main(args.claims, args.out)
