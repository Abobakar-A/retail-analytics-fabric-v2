# =============================================================================
# Retail Analytics Platform v2 — Gold Layer
# Notebook: 03_build_gold
# Author: Abobakar Suliman
#
# Reads from Silver Delta tables and builds business-ready aggregates.
# Gold tables are always fully rebuilt from Silver (idempotent by design —
# they are pure functions of Silver data, so re-running is always safe).
#
# Tables produced:
#   - gold_revenue_by_category  : revenue/profit by category, region, month
#   - gold_customer_segments    : performance by customer segment and city
#   - gold_store_performance    : store-level revenue and discount analysis
#   - gold_product_performance  : product-level sales volume and profitability
#
# Schema reference: docs/schema_contract.md
# =============================================================================


# -----------------------------------------------------------------------------
# CELL 1 — Load Silver tables
#
# Note: For dimension tables, we filter to is_current = True.
# This gives us the latest version of each dimension record for reporting.
# Historical analysis (e.g. "what segment was this customer in last year?")
# would require joining on effective_date/end_date instead — that's an
# advanced use case handled at the semantic model layer, not here.
# -----------------------------------------------------------------------------

from pyspark.sql.functions import col, round, sum, count, avg

dim_date     = spark.table("dim_date")
dim_customer = spark.table("dim_customer").filter(col("is_current") == True)
dim_product  = spark.table("dim_product").filter(col("is_current") == True)
dim_store    = spark.table("dim_store").filter(col("is_current") == True)
fact_sale    = spark.table("fact_sale")

print("✅ Silver tables loaded")
print(f"   fact_sale:    {fact_sale.count()} rows")
print(f"   dim_customer: {dim_customer.count()} rows (current versions only)")
print(f"   dim_product:  {dim_product.count()} rows (current versions only)")
print(f"   dim_store:    {dim_store.count()} rows (current versions only)")
print(f"   dim_date:     {dim_date.count()} rows")


# -----------------------------------------------------------------------------
# CELL 2 — Build enriched sales view
#
# Join fact_sale to all dimensions to create a single enriched DataFrame
# that all Gold aggregates are built from. This avoids repeating the join
# logic in every Gold table.
# -----------------------------------------------------------------------------

sales_enriched = fact_sale.alias("f") \
    .join(dim_date.alias("d"),     col("f.date_id")     == col("d.date_id"),     "left") \
    .join(dim_customer.alias("c"), col("f.customer_sk") == col("c.customer_sk"), "left") \
    .join(dim_product.alias("p"),  col("f.product_sk")  == col("p.product_sk"),  "left") \
    .join(dim_store.alias("st"),   col("f.store_sk")    == col("st.store_sk"),   "left")

print("✅ sales_enriched —", sales_enriched.count(), "rows")


# -----------------------------------------------------------------------------
# CELL 3 — gold_revenue_by_category
# Business question: Which product categories drive the most revenue,
# broken down by region and month?
# -----------------------------------------------------------------------------

gold_revenue_by_category = sales_enriched \
    .groupBy("year", "month_name", "month_num", "category", "region") \
    .agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        round(sum("profit"), 2).alias("total_profit"),
        round(avg(col("p.unit_price") - col("p.unit_cost")), 2).alias("avg_margin"),
        count("sale_id").alias("total_orders")
    ).orderBy("year", "month_num")

gold_revenue_by_category.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_revenue_by_category")

print("✅ gold_revenue_by_category —", gold_revenue_by_category.count(), "rows")


# -----------------------------------------------------------------------------
# CELL 4 — gold_customer_segments
# Business question: Which customer segments and cities generate
# the most revenue and orders?
# -----------------------------------------------------------------------------

gold_customer_segments = sales_enriched \
    .groupBy("year", "month_name", "month_num", "segment", col("c.city").alias("customer_city")) \
    .agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        round(sum("profit"), 2).alias("total_profit"),
        count("sale_id").alias("total_orders")
    ).orderBy("year", "month_num")

gold_customer_segments.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_customer_segments")

print("✅ gold_customer_segments —", gold_customer_segments.count(), "rows")


# -----------------------------------------------------------------------------
# CELL 5 — gold_store_performance
# Business question: Which stores perform best, and how does discounting
# vary across stores and regions?
# -----------------------------------------------------------------------------

gold_store_performance = sales_enriched \
    .groupBy(
        "year", "month_name", "month_num",
        col("st.store_name").alias("store_name"),
        col("st.city").alias("store_city"),
        "region"
    ) \
    .agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        round(sum("profit"), 2).alias("total_profit"),
        count("sale_id").alias("total_orders"),
        round(avg("discount_pct"), 2).alias("avg_discount_pct")
    ).orderBy("year", "month_num")

gold_store_performance.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_store_performance")

print("✅ gold_store_performance —", gold_store_performance.count(), "rows")


# -----------------------------------------------------------------------------
# CELL 6 — gold_product_performance
# Business question: Which products and categories sell the most units
# and generate the most profit?
# -----------------------------------------------------------------------------

gold_product_performance = sales_enriched \
    .groupBy("year", "category", "brand", col("p.product_name").alias("product_name")) \
    .agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        round(sum("profit"), 2).alias("total_profit"),
        count("sale_id").alias("total_orders"),
        sum("quantity").alias("total_units_sold")
    ).orderBy("year")

gold_product_performance.write.format("delta").mode("overwrite") \
    .saveAsTable("gold_product_performance")

print("✅ gold_product_performance —", gold_product_performance.count(), "rows")
print("\n🥇 Gold layer complete!")
