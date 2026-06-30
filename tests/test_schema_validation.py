"""
Basic validation tests for the retail analytics schema contract.
These tests check that the schema contract document exists and is
well-formed, and will be extended as notebooks are added.
"""

import os


def test_schema_contract_exists():
    """The schema contract must exist as the single source of truth."""
    assert os.path.isfile("docs/schema_contract.md"), \
        "docs/schema_contract.md is missing"


def test_schema_contract_not_empty():
    """The schema contract must have actual content."""
    with open("docs/schema_contract.md", "r") as f:
        content = f.read()
    assert len(content) > 100, \
        "schema_contract.md appears to be empty or too short"


def test_schema_contract_defines_all_tables():
    """Every table in our data model must be documented."""
    with open("docs/schema_contract.md", "r") as f:
        content = f.read()

    required_tables = [
        "dim_date",
        "dim_customer",
        "dim_product",
        "dim_store",
        "fact_sale",
    ]

    for table in required_tables:
        assert table in content, \
            f"Table '{table}' is not documented in schema_contract.md"
