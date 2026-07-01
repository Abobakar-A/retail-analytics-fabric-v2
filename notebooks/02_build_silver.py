# =============================================================================
# Retail Analytics Platform v2 — Silver Layer
# Notebook: 02_build_silver
# Author: Abobakar Suliman
#
# Reads raw Bronze CSV batches and builds clean, typed Silver Delta tables:
#   - dim_date      : calendar dimension (generated, not from source)
#   - dim_customer  : SCD Type 2 — tracks city and segment changes
#   - dim_product   : SCD Type 2 — tracks unit_price and unit_cost changes
#   - dim_store     : SCD Type 2 — tracks city and region changes
#   - fact_sale     : transactional fact table, joined to correct dimension
#                     versions at the time of each sale
#
# Schema reference: docs/schema_contract.md
# =============================================================================


# -----------------------------------------------------------------------------
# CELL 1 — Configuration & Helper Functions
# -----------------------------------------------------------------------------

import os
import pandas as pd
from pyspark.sql.functions import col, to_date, lag, row_number, when, lit, round
from pyspark.sql.window import Window

BATCH_DATES = ["2024-01-01", "2024-01-02", "2024-01-03"]
BRONZE_PATH = "Files/bronze/"


def load_bronze_snapshots(entity: str, dates: list):
    """
    Loads and stacks multiple daily CSV snapshots for a given entity.
    Each file represents one day's immutable batch landing in Bronze.

    Args:
        entity: table name prefix (e.g. 'customers', 'products')
        dates: list of batch date strings in YYYY-MM-DD format

    Returns:
        A single stacked PySpark DataFrame with all daily snapshots combined.
    """
    dfs = []
    for d in dates:
        path = f"{BRONZE_PATH}{entity}_{d}.csv"
        dfs.append(
            spark.read.option("header", True).option("inferSchema", True).csv(path)
        )
    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)
    return result


def build_scd2_dimension(raw_df, business_key: str, tracked_columns: list, other_columns: list):
    """
    Builds an SCD Type 2 dimension table from stacked daily snapshots.

    SCD2 means: when a tracked attribute changes, we keep the old record
    (with an end_date) and insert a new record (with is_current = True).
    This preserves the full history so that fact records always point to
    the dimension version that was active at the time of the transaction.

    Args:
        raw_df:          stacked raw DataFrame with a 'batch_date' column
        business_key:    the natural/business key (e.g. 'customer_id')
        tracked_columns: columns that trigger a new version when changed
        other_columns:   columns stored in the table but don't trigger versioning

    Returns:
        DataFrame with: surrogate key, business key, all attributes,
        effective_date, end_date, is_current — per the schema contract.
    """
    # Step 1 — Cast batch_date to proper DateType
    typed_df = raw_df.withColumn("batch_date", to_date(col("batch_date"), "yyyy-MM-dd"))

    window_by_key = Window.partitionBy(business_key).orderBy("batch_date")

    # Step 2 — For each tracked column, compare current row to previous row
    # using LAG() window function. If any tracked column changed, this row
    # is a "new version" of the entity.
    df_with_prev = typed_df
    change_condition = lit(False)

    for tracked_col in tracked_columns:
        prev_col_name = f"prev_{tracked_col}"
        df_with_prev = df_with_prev.withColumn(
            prev_col_name, lag(tracked_col).over(window_by_key)
        )
        change_condition = (
            change_condition
            | col(prev_col_name).isNull()          # first time we see this entity
            | (col(tracked_col) != col(prev_col_name))  # value actually changed
        )

    df_with_prev = df_with_prev.withColumn("is_new_version", change_condition)

    # Step 3 — Keep only "new version" rows (collapse duplicates)
    all_attribute_cols = [business_key] + tracked_columns + other_columns

    versions = df_with_prev.filter(col("is_new_version") == True) \
        .select(*all_attribute_cols, "batch_date") \
        .withColumnRenamed("batch_date", "effective_date")

    # Step 4 — Calculate end_date: the effective_date of the NEXT version
    # for the same entity. NULL end_date means this is the current version.
    window_next = Window.partitionBy(business_key).orderBy("effective_date")
    versions = versions \
        .withColumn("end_date", lag("effective_date", -1).over(window_next)) \
        .withColumn("is_current", col("end_date").isNull())

    # Step 5 — Add a surrogate key (unique per version, per schema contract)
    window_sk = Window.orderBy(business_key, "effective_date")
    versions = versions.withColumn("row_sk", row_number().over(window_sk))

    final_cols = ["row_sk"] + all_attribute_cols + ["effective_date", "end_date", "is_current"]
    return versions.select(*final_cols)


print("✅ Configuration and helper functions loaded")


# -----------------------------------------------------------------------------
# CELL 2 — dim_date (generated calendar, not from source data)
# -----------------------------------------------------------------------------

# dim_date is not sourced from Bronze — it's a generated calendar table.
# We generate it wide enough (2023-2025) to cover past and future data
# without needing to rebuild it as the project grows.

dates = pd.date_range(start="2023-01-01", end="2025-12-31", freq="D")

dim_date = pd.DataFrame({
    "date_id":      dates.strftime("%Y%m%d").astype(int),  # integer FK for fact table joins
    "full_date":    dates.strftime("%Y-%m-%d"),            # string date (will be cast to DateType in semantic model)
    "day":          dates.day,
    "month_num":    dates.month,
    "month_name":   dates.strftime("%B"),                  # "January", "February"...
    "month_short":  dates.strftime("%b"),                  # "Jan", "Feb"...
    "quarter":      dates.quarter,
    "year":         dates.year,
    "weekday_name": dates.strftime("%A"),                  # "Monday", "Tuesday"...
    "is_weekend":   (dates.dayofweek >= 5).astype(bool),   # True for Saturday/Sunday
})

spark.createDataFrame(dim_date) \
    .write.format("delta").mode("overwrite").saveAsTable("dim_date")

print("✅ dim_date —", len(dim_date), "rows (2023-2025 calendar)")


# -----------------------------------------------------------------------------
# CELL 3 — dim_customer (SCD Type 2)
# Tracked attributes: city, segment
# Rationale: customers can move cities or change purchasing tier over time.
# Untracked (stored but don't trigger versioning): first_name, email, join_date
# -----------------------------------------------------------------------------

customers_raw = load_bronze_snapshots("customers", BATCH_DATES)

dim_customer = build_scd2_dimension(
    raw_df=customers_raw,
    business_key="customer_id",
    tracked_columns=["city", "segment"],
    other_columns=["first_name", "email", "join_date"]
)
dim_customer = dim_customer.withColumnRenamed("row_sk", "customer_sk")
dim_customer.write.format("delta").mode("overwrite").saveAsTable("dim_customer")

print("✅ dim_customer —", dim_customer.count(), "rows")
print("   (base: 1000 customers + extra rows for each historical version)")


# -----------------------------------------------------------------------------
# CELL 4 — dim_product (SCD Type 2)
# Tracked attributes: unit_price, unit_cost
# Rationale: product prices change over time. Tracking this means historical
# sales correctly reflect the price at time of purchase, not today's price.
# Untracked: product_name, category, brand (stable identifiers)
# -----------------------------------------------------------------------------

products_raw = load_bronze_snapshots("products", BATCH_DATES)

dim_product = build_scd2_dimension(
    raw_df=products_raw,
    business_key="product_id",
    tracked_columns=["unit_price", "unit_cost"],
    other_columns=["product_name", "category", "brand"]
)
dim_product = dim_product.withColumnRenamed("row_sk", "product_sk")
dim_product.write.format("delta").mode("overwrite").saveAsTable("dim_product")

print("✅ dim_product —", dim_product.count(), "rows")
print("   (base: 200 products + extra rows for each price change)")


# -----------------------------------------------------------------------------
# CELL 5 — dim_store (SCD Type 2)
# Tracked attributes: city, region
# Rationale: stores can relocate or be reassigned to a different region.
# Untracked: store_name, open_date (stable identifiers)
# -----------------------------------------------------------------------------

stores_raw = load_bronze_snapshots("stores", BATCH_DATES)

dim_store = build_scd2_dimension(
    raw_df=stores_raw,
    business_key="store_id",
    tracked_columns=["city", "region"],
    other_columns=["store_name", "open_date"]
)
dim_store = dim_store.withColumnRenamed("row_sk", "store_sk")
dim_store.write.format("delta").mode("overwrite").saveAsTable("dim_store")

print("✅ dim_store —", dim_store.count(), "rows")
print("   (base: 20 stores + extra rows for each relocation)")


# -----------------------------------------------------------------------------
# CELL 6 — fact_sale
#
# Key design decision: we join each sale to the dimension VERSION that was
# active at the time of the sale using a temporal join:
#   sale_date >= effective_date AND (end_date IS NULL OR sale_date < end_date)
#
# This means if a customer changed city on 2024-01-02, a sale from
# 2024-01-01 correctly links to their "before" record, and a sale from
# 2024-01-02 correctly links to their "after" record.
# This is the core value of SCD2 in a fact table.
# -----------------------------------------------------------------------------

sales_raw = load_bronze_snapshots("sales", BATCH_DATES)

sales_typed = sales_raw \
    .withColumn("date_id",      col("date_id").cast("int")) \
    .withColumn("customer_id",  col("customer_id").cast("int")) \
    .withColumn("product_id",   col("product_id").cast("int")) \
    .withColumn("store_id",     col("store_id").cast("int")) \
    .withColumn("quantity",     col("quantity").cast("int")) \
    .withColumn("discount_pct", col("discount_pct").cast("int")) \
    .withColumn("sale_date",    to_date(col("batch_date"), "yyyy-MM-dd")) \
    .withColumn("batch_date",   to_date(col("batch_date"), "yyyy-MM-dd"))

fact = sales_typed.alias("s") \
    .join(
        dim_customer.alias("c"),
        (col("s.customer_id") == col("c.customer_id")) &
        (col("s.sale_date") >= col("c.effective_date")) &
        (col("c.end_date").isNull() | (col("s.sale_date") < col("c.end_date"))),
        "left"
    ) \
    .join(
        dim_product.alias("p"),
        (col("s.product_id") == col("p.product_id")) &
        (col("s.sale_date") >= col("p.effective_date")) &
        (col("p.end_date").isNull() | (col("s.sale_date") < col("p.end_date"))),
        "left"
    ) \
    .join(
        dim_store.alias("st"),
        (col("s.store_id") == col("st.store_id")) &
        (col("s.sale_date") >= col("st.effective_date")) &
        (col("st.end_date").isNull() | (col("s.sale_date") < col("st.end_date"))),
        "left"
    ) \
    .withColumn("discount_multiplier", round(1 - col("s.discount_pct") / 100, 2)) \
    .withColumn("revenue", round(col("s.quantity") * col("p.unit_price") * col("discount_multiplier"), 2)) \
    .withColumn("cost",    round(col("s.quantity") * col("p.unit_cost"), 2)) \
    .withColumn("profit",  round(col("revenue") - col("cost"), 2)) \
    .select(
        col("s.sale_id").cast("int"),
        col("s.date_id"),
        col("c.customer_sk"),
        col("p.product_sk"),
        col("st.store_sk"),
        col("s.quantity"),
        col("s.discount_pct"),
        col("revenue"),
        col("cost"),
        col("profit"),
        col("s.batch_date")
    )

fact.write.format("delta").mode("overwrite").saveAsTable("fact_sale")

print("✅ fact_sale —", fact.count(), "rows")
print("\n🥈 Silver layer complete!")
