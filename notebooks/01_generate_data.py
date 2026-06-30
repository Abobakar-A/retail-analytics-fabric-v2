# =============================================================================
# Retail Analytics Platform v2 — Data Generator
# Notebook: 01_generate_data
# Author: Abobakar Suliman
#
# Generates synthetic retail data in daily batches to simulate a real
# production pipeline. Each batch call:
#   - Uses a fixed seed derived from batch_date (idempotent: same date
#     always produces the same data)
#   - Introduces occasional dimension changes (to exercise SCD2 logic)
#   - Appends new fact records only (to exercise incremental MERGE logic)
#
# Schema reference: docs/schema_contract.md
# =============================================================================

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

BRONZE_PATH = "/lakehouse/default/Files/bronze/"

CITIES = ["Dubai", "Abu Dhabi", "Sharjah", "London", "New York", "Paris", "Berlin", "Tokyo"]
SEGMENTS = ["Retail", "Wholesale", "Online"]
CATEGORIES = ["Electronics", "Clothing", "Food & Beverage", "Home & Garden", "Sports"]
BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]
REGIONS = ["MENA", "Europe", "Americas", "Asia"]

N_CUSTOMERS = 1000
N_PRODUCTS = 200
N_STORES = 20
SALES_PER_DAY = 150  # roughly, with randomness


def _seed_for_batch(batch_date: str) -> int:
    """Deterministic seed derived from the batch date string.
    Ensures the same batch_date always generates identical data —
    this is what makes the generator idempotent.
    """
    return int(batch_date.replace("-", ""))


# -----------------------------------------------------------------------------
# Initial dimension generation (first batch only)
# -----------------------------------------------------------------------------

def generate_initial_customers(batch_date: str) -> pd.DataFrame:
    seed = _seed_for_batch(batch_date)
    rng = np.random.default_rng(seed)

    return pd.DataFrame({
        "customer_id": range(1, N_CUSTOMERS + 1),
        "first_name": [f"Customer_{i}" for i in range(1, N_CUSTOMERS + 1)],
        "email": [f"customer_{i}@email.com" for i in range(1, N_CUSTOMERS + 1)],
        "city": rng.choice(CITIES, N_CUSTOMERS),
        "segment": rng.choice(SEGMENTS, N_CUSTOMERS),
        "join_date": [
            (datetime(2020, 1, 1) + timedelta(days=int(rng.integers(0, 1400)))).strftime("%Y-%m-%d")
            for _ in range(N_CUSTOMERS)
        ],
        "batch_date": batch_date,
    })


def generate_initial_products(batch_date: str) -> pd.DataFrame:
    seed = _seed_for_batch(batch_date) + 1
    rng = np.random.default_rng(seed)

    unit_cost = np.round(rng.uniform(5, 500, N_PRODUCTS), 2)
    unit_price = np.round(unit_cost * rng.uniform(1.2, 2.5, N_PRODUCTS), 2)  # always > cost

    return pd.DataFrame({
        "product_id": range(1, N_PRODUCTS + 1),
        "product_name": [f"Product_{i}" for i in range(1, N_PRODUCTS + 1)],
        "category": rng.choice(CATEGORIES, N_PRODUCTS),
        "brand": rng.choice(BRANDS, N_PRODUCTS),
        "unit_cost": unit_cost,
        "unit_price": unit_price,
        "batch_date": batch_date,
    })


def generate_initial_stores(batch_date: str) -> pd.DataFrame:
    seed = _seed_for_batch(batch_date) + 2
    rng = np.random.default_rng(seed)

    return pd.DataFrame({
        "store_id": range(1, N_STORES + 1),
        "store_name": [f"Store_{i}" for i in range(1, N_STORES + 1)],
        "city": rng.choice(CITIES, N_STORES),
        "region": rng.choice(REGIONS, N_STORES),
        "open_date": [
            (datetime(2018, 1, 1) + timedelta(days=int(rng.integers(0, 1000)))).strftime("%Y-%m-%d")
            for _ in range(N_STORES)
        ],
        "batch_date": batch_date,
    })


# -----------------------------------------------------------------------------
# Dimension mutation (simulates real-world changes for SCD2 testing)
# -----------------------------------------------------------------------------

def mutate_customers(customers_df: pd.DataFrame, batch_date: str, change_pct: float = 0.05) -> pd.DataFrame:
    """Randomly changes city/segment for a small percentage of customers.
    This simulates real customers moving cities or changing tiers —
    exactly what SCD2 is designed to capture.
    """
    seed = _seed_for_batch(batch_date) + 100
    rng = np.random.default_rng(seed)

    df = customers_df.copy()
    n_changes = int(len(df) * change_pct)
    changed_idx = rng.choice(df.index, size=n_changes, replace=False)

    df.loc[changed_idx, "city"] = rng.choice(CITIES, n_changes)
    df.loc[changed_idx, "segment"] = rng.choice(SEGMENTS, n_changes)
    df["batch_date"] = batch_date

    return df


def mutate_products(products_df: pd.DataFrame, batch_date: str, change_pct: float = 0.03) -> pd.DataFrame:
    """Randomly changes price for a small percentage of products."""
    seed = _seed_for_batch(batch_date) + 101
    rng = np.random.default_rng(seed)

    df = products_df.copy()
    n_changes = int(len(df) * change_pct)
    changed_idx = rng.choice(df.index, size=n_changes, replace=False)

    df.loc[changed_idx, "unit_price"] = np.round(
        df.loc[changed_idx, "unit_price"] * rng.uniform(0.9, 1.15, n_changes), 2
    )
    df["batch_date"] = batch_date

    return df


def mutate_stores(stores_df: pd.DataFrame, batch_date: str, change_pct: float = 0.02) -> pd.DataFrame:
    """Randomly changes region for a small percentage of stores."""
    seed = _seed_for_batch(batch_date) + 102
    rng = np.random.default_rng(seed)

    df = stores_df.copy()
    n_changes = max(1, int(len(df) * change_pct))
    changed_idx = rng.choice(df.index, size=n_changes, replace=False)

    df.loc[changed_idx, "region"] = rng.choice(REGIONS, n_changes)
    df["batch_date"] = batch_date

    return df


# -----------------------------------------------------------------------------
# Fact data generation (new sales for a single day)
# -----------------------------------------------------------------------------

def generate_daily_sales(batch_date: str, sale_id_start: int) -> pd.DataFrame:
    """Generates a single day's worth of sales transactions.
    sale_id_start ensures sale_ids never collide across batches.
    """
    seed = _seed_for_batch(batch_date) + 200
    rng = np.random.default_rng(seed)

    n_sales = int(rng.integers(SALES_PER_DAY - 30, SALES_PER_DAY + 30))
    date_id = int(batch_date.replace("-", ""))

    return pd.DataFrame({
        "sale_id": range(sale_id_start, sale_id_start + n_sales),
        "date_id": date_id,
        "customer_id": rng.integers(1, N_CUSTOMERS + 1, n_sales),
        "product_id": rng.integers(1, N_PRODUCTS + 1, n_sales),
        "store_id": rng.integers(1, N_STORES + 1, n_sales),
        "quantity": rng.integers(1, 20, n_sales),
        "discount_pct": rng.choice([0, 5, 10, 15, 20], n_sales),
        "batch_date": batch_date,
    })


# -----------------------------------------------------------------------------
# Orchestration — run a single batch end to end
# -----------------------------------------------------------------------------

def run_batch(batch_date: str, is_first_batch: bool, sale_id_start: int):
    """Generates and writes one day's worth of bronze data.

    On the first batch: generates fresh dimensions.
    On subsequent batches: reads previous dimensions and applies mutations,
    so we get realistic, evolving reference data over time.
    """
    print(f"=== Running batch: {batch_date} (first_batch={is_first_batch}) ===")

    if is_first_batch:
        customers = generate_initial_customers(batch_date)
        products = generate_initial_products(batch_date)
        stores = generate_initial_stores(batch_date)
    else:
        # Read most recent bronze snapshot, then mutate it
        prev_customers = pd.read_csv(f"{BRONZE_PATH}customers_latest.csv")
        prev_products = pd.read_csv(f"{BRONZE_PATH}products_latest.csv")
        prev_stores = pd.read_csv(f"{BRONZE_PATH}stores_latest.csv")

        customers = mutate_customers(prev_customers, batch_date)
        products = mutate_products(prev_products, batch_date)
        stores = mutate_stores(prev_stores, batch_date)

    sales = generate_daily_sales(batch_date, sale_id_start)

    # Write batch-specific files (immutable, append-only history)
    customers.to_csv(f"{BRONZE_PATH}customers_{batch_date}.csv", index=False)
    products.to_csv(f"{BRONZE_PATH}products_{batch_date}.csv", index=False)
    stores.to_csv(f"{BRONZE_PATH}stores_{batch_date}.csv", index=False)
    sales.to_csv(f"{BRONZE_PATH}sales_{batch_date}.csv", index=False)

    # Write "latest" snapshot files (used as input to the next batch's mutation step)
    customers.to_csv(f"{BRONZE_PATH}customers_latest.csv", index=False)
    products.to_csv(f"{BRONZE_PATH}products_latest.csv", index=False)
    stores.to_csv(f"{BRONZE_PATH}stores_latest.csv", index=False)

    print(f"✅ customers: {len(customers)} rows")
    print(f"✅ products:  {len(products)} rows")
    print(f"✅ stores:    {len(stores)} rows")
    print(f"✅ sales:     {len(sales)} rows (sale_id {sale_id_start}-{sale_id_start + len(sales) - 1})")

    return sale_id_start + len(sales)


# -----------------------------------------------------------------------------
# Example usage — generate 3 days of batches
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    next_sale_id = 1
    next_sale_id = run_batch("2024-01-01", is_first_batch=True, sale_id_start=next_sale_id)
    next_sale_id = run_batch("2024-01-02", is_first_batch=False, sale_id_start=next_sale_id)
    next_sale_id = run_batch("2024-01-03", is_first_batch=False, sale_id_start=next_sale_id)

    print("\n🎉 All batches generated successfully.")
