# Retail Analytics Platform v2 — Microsoft Fabric

An enterprise-grade data engineering project built on Microsoft Fabric,
demonstrating production patterns including idempotent batch ingestion,
SCD Type 2 dimensions, a proper star schema, and a CI/CD workflow with
automated schema validation.

This is a deliberate rebuild of v1 (available at retail-analytics-fabric),
designed to fix the architectural gaps identified in the first iteration
and apply real enterprise standards from the ground up.

---

## Architecture
```
Raw Sources
↓
Bronze Layer (OneLake Files)
Immutable daily CSV batches, partitioned by date
customers_YYYY-MM-DD.csv | products_YYYY-MM-DD.csv
stores_YYYY-MM-DD.csv    | sales_YYYY-MM-DD.csv
↓
Silver Layer (Delta Tables)
dim_date      — 1,096 rows, 2023-2025 calendar
dim_customer  — SCD Type 2 (tracks city, segment changes)
dim_product   — SCD Type 2 (tracks unit_price, unit_cost changes)
dim_store     — SCD Type 2 (tracks city, region changes)
fact_sale     — 465 rows, joined to correct dimension versions
↓
Gold Layer (Delta Tables)
gold_revenue_by_category
gold_customer_segments
gold_store_performance
gold_product_performance
```
---

## Tech Stack

| Layer | Technology |
|---|---|
| Platform | Microsoft Fabric |
| Storage | OneLake (Delta format) |
| Transformation | PySpark (Fabric Notebooks) |
| Version Control | GitHub (branch protection, PR workflow) |
| CI/CD | GitHub Actions (schema validation + pytest) |
| Semantic Layer | Direct Lake Semantic Model |
| Visualization | Power BI |

---

## Key Engineering Decisions

### 1. Schema Contract First
Before writing any code, a schema contract (`docs/schema_contract.md`)
was defined — column names, data types, and key relationships for every
table. All notebooks reference this contract. Any schema change requires
a PR update to the contract first.

This directly fixed the root cause of v1's bugs (mismatched data types
between tables causing silent join failures in Power BI Direct Lake mode).

### 2. Idempotent Batch Generator
Data is generated in daily batches using a deterministic seed derived
from the batch date (`int("2024-01-01".replace("-", "")) = 20240101`).
Running the same batch date twice always produces identical output —
no duplicates, no drift. This is the foundation of safe pipeline reruns.

### 3. SCD Type 2 on All Dimensions
All three dimension tables track historical changes:
- `dim_customer` tracks city and segment changes
- `dim_product` tracks unit_price and unit_cost changes
- `dim_store` tracks city and region changes

Each version gets its own surrogate key (`customer_sk`, `product_sk`,
`store_sk`). The fact table joins to the surrogate key of the version
that was active *at the time of the sale*, not the current version.

This means historical sales correctly reflect the customer's city at
the time of purchase — not where they live today.

### 4. Temporal Join in fact_sale
```python
fact.join(
    dim_customer,
    (sale.customer_id == dim_customer.customer_id) &
    (sale.sale_date >= dim_customer.effective_date) &
    (dim_customer.end_date.isNull() | (sale.sale_date < dim_customer.end_date)),
    "left"
)
```
This is the correct SCD2-aware join pattern. Most tutorials get this
wrong by joining only on the natural key (customer_id), which returns
duplicate rows when a customer has multiple versions.

### 5. CI/CD with GitHub Actions
Every Pull Request triggers an automated workflow that:
- Confirms the schema contract document exists
- Runs pytest schema validation tests
- Blocks merge if any check fails

`main` branch is protected — no direct pushes allowed.

---

## Data Model

### Bronze Layer — Raw Ingestion
Each entity has one file per batch date (immutable) plus a `_latest`
snapshot used as input to the next batch's mutation step:

| Entity | Rows per batch | Mutation rate |
|---|---|---|
| customers | 1,000 | 5% change city/segment per batch |
| products | 200 | 3% change unit_price per batch |
| stores | 20 | 2% change city/region per batch |
| sales | ~150/day | Append only, no mutations |

### Silver Layer — Cleaned Delta Tables
| Table | Rows | Description |
|---|---|---|
| dim_date | 1,096 | Generated calendar 2023-2025 |
| dim_customer | 1,098 | 1,000 customers + 98 historical versions |
| dim_product | 212 | 200 products + 12 price change versions |
| dim_store | 22 | 20 stores + 2 relocation versions |
| fact_sale | 465 | 3 days of sales, SCD2-aware joins |

### Gold Layer — Business Aggregates
| Table | Rows | Business Question |
|---|---|---|
| gold_revenue_by_category | 29 | Which categories drive revenue by region? |
| gold_customer_segments | 25 | Which segments spend the most? |
| gold_store_performance | 21 | Which stores perform best? |
| gold_product_performance | 184 | Which products sell the most units? |

---
## Repository Structure
```
retail-analytics-fabric-v2/
├── .github/
│   └── workflows/
│       └── validate.yml          CI: runs on every PR
├── docs/
│   └── schema_contract.md        Single source of truth for all schemas
├── notebooks/
│   ├── 01_generate_data.py       Bronze: idempotent batch data generator
│   ├── 02_build_silver.py        Silver: SCD2 dimensions + fact table
│   └── 03_build_gold.py          Gold: business aggregates
├── tests/
│   └── test_schema_validation.py Schema contract validation tests
└── README.md
```
---

## Known Limitations & Production Improvements

| Current | Production |
|---|---|
| 3 days of synthetic data | Real source systems via Fabric Data Factory |
| Manual notebook runs | Scheduled Fabric Data Pipeline with alerts |
| Full fact_sale rebuild each run | Incremental MERGE on fact_sale only (dimension full rebuild is intentional — required for correct SCD2 end_date calculation) |
| GitHub manual sync | Fabric Git Integration (requires paid capacity) |
| No row-level security | RLS by region or business unit in Power BI |
| No data quality checks | Great Expectations or Fabric Data Quality rules |

---

## What I Learned Building This

v1 taught me what breaks in production:
- Implicit schema inference causes silent type mismatches
- Pre-aggregated Gold tables can't cross-filter in Power BI
- Adding SCD2 as an afterthought is much harder than designing for it upfront

v2 was built to fix all of those — starting with a schema contract,
designing the star schema before writing any transformation code, and
building SCD2 into the dimension logic from day one.

---

## Author

**Abobakar Suliman** — Data Engineer
Microsoft Certified Fabric Analytics Engineer Associate (DP-700)

📧 abobakarsuliman28@gmail.com
📍 Dubai, UAE
🔗 github.com/Abobakar-A/retail-analytics-fabric-v2


