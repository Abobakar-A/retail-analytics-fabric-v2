# Schema Contract

This document is the single source of truth for all table schemas in this project.
Every notebook, every PySpark DataFrame, and every Power BI relationship must conform
to the types defined here. No implicit type inference is allowed — all schemas are
explicitly declared in code against this contract.

Last updated: 2026-06-30

---

## dim_date

| Column        | Type        | Notes                                  |
|---------------|-------------|-----------------------------------------|
| date_id       | IntegerType | Surrogate key, format YYYYMMDD          |
| full_date     | DateType    | Proper date type, not string            |
| day           | IntegerType |                                          |
| month_num     | IntegerType |                                          |
| month_name    | StringType  |                                          |
| month_short   | StringType  |                                          |
| quarter       | IntegerType |                                          |
| year          | IntegerType |                                          |
| weekday_name  | StringType  |                                          |
| is_weekend    | BooleanType |                                          |

**Marked as Power BI date table on `full_date`.**

---

## dim_customer (SCD Type 2)

| Column            | Type        | Notes                                       |
|-------------------|-------------|-----------------------------------------------|
| customer_sk       | IntegerType | Surrogate key, unique per version             |
| customer_id       | IntegerType | Natural/business key, stable across versions  |
| first_name        | StringType  |                                                |
| email             | StringType  |                                                |
| city              | StringType  | Tracked attribute (SCD2 trigger)              |
| segment           | StringType  | Tracked attribute (SCD2 trigger)              |
| join_date         | DateType    |                                                |
| effective_date    | DateType    | SCD2 — when this version became active        |
| end_date          | DateType    | SCD2 — null if current version                |
| is_current        | BooleanType | SCD2 — true for the active version             |

---

## dim_product (SCD Type 2)

| Column            | Type        | Notes                                       |
|-------------------|-------------|-----------------------------------------------|
| product_sk        | IntegerType | Surrogate key                                  |
| product_id        | IntegerType | Natural key                                    |
| product_name      | StringType  |                                                |
| category          | StringType  |                                                |
| brand             | StringType  |                                                |
| unit_cost         | DoubleType  | Tracked attribute (SCD2 trigger)              |
| unit_price        | DoubleType  | Tracked attribute (SCD2 trigger)              |
| effective_date    | DateType    | SCD2                                           |
| end_date          | DateType    | SCD2                                           |
| is_current        | BooleanType | SCD2                                           |

---

## dim_store (SCD Type 2)

| Column            | Type        | Notes                                       |
|-------------------|-------------|-----------------------------------------------|
| store_sk          | IntegerType | Surrogate key                                  |
| store_id          | IntegerType | Natural key                                    |
| store_name        | StringType  |                                                |
| city              | StringType  | Tracked attribute (SCD2 trigger)              |
| region            | StringType  |                                                |
| open_date         | DateType    |                                                |
| effective_date    | DateType    | SCD2                                           |
| end_date          | DateType    | SCD2                                           |
| is_current        | BooleanType | SCD2                                           |

---

## fact_sale

| Column          | Type        | Notes                                              |
|-----------------|-------------|------------------------------------------------------|
| sale_id         | IntegerType | Natural key, unique per transaction                   |
| date_id         | IntegerType | FK → dim_date.date_id                                 |
| customer_sk     | IntegerType | FK → dim_customer.customer_sk (version at sale time)  |
| product_sk      | IntegerType | FK → dim_product.product_sk (version at sale time)    |
| store_sk        | IntegerType | FK → dim_store.store_sk (version at sale time)        |
| quantity        | IntegerType |                                                        |
| discount_pct    | IntegerType |                                                        |
| revenue         | DoubleType  |                                                        |
| cost            | DoubleType  |                                                        |
| profit          | DoubleType  |                                                        |
| batch_date      | DateType    | Ingestion batch tracking, supports idempotency checks |

---

## Design Principles

1. **All foreign keys must match the exact type of their referenced primary key.**
   This was the root cause of a bug in v1 (date_id as bigint vs int across tables).

2. **Surrogate keys (`_sk`) vs natural keys (`_id`):**
   Fact tables join to surrogate keys, which represent a specific version of a
   dimension record. Natural keys represent the real-world entity across all
   its versions over time.

3. **No implicit schema inference.**
   Every DataFrame in every notebook explicitly declares its schema using
   PySpark `StructType`, matching this contract exactly.

4. **Schema changes require a PR.**
   Any change to this document must go through a pull request and be reflected
   in the corresponding notebook code in the same PR.
