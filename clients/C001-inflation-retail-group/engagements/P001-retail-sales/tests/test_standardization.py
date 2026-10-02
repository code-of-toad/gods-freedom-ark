"""
Tests for P001 standardization.
"""
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal
from pyspark.sql import functions as F

from p001_retail_sales.ingestion import (
    read_raw_sales,
    read_raw_products,
    read_raw_stores,
)
from p001_retail_sales.standardization import (
    standardize_sales,
    standardize_products,
    standardize_stores,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_INPUT = PROJECT_ROOT / 'data' / 'test' / 'input'


def test_standardize_sales_parses_valid_quantity(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv')
    )

    standardized_df = standardize_sales(raw_df)
    row = (
        standardized_df
        .filter(
            (F.col('order_id') == 'O1001') & (F.col('line_id') == 1)
        )
        .first()
    )

    assert row['raw_quantity'] == '2'
    assert row['quantity'] == 2


def test_standardize_sales_preserves_failed_quantity_parse(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv')
    )

    standardized_df = standardize_sales(raw_df)
    row = (
        standardized_df
        .filter(F.col('order_id') == 'O1007')
        .first()
    )

    assert row['raw_quantity'] == 'five'
    assert row['quantity'] is None


def test_standardize_sales_parses_canonical_types(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv')
    )
    standardized_df = standardize_sales(raw_df)

    row = (
        standardized_df
        .filter((F.col('order_id') == 'O1001') & (F.col('line_id') == 1))
        .first()
    )

    assert row['sale_date'] == date(2026, 10, 1)
    assert row['unit_price'] == Decimal('5.00')
    assert row['unit_cost'] == Decimal('3.00')
    assert row['discount_amount'] == Decimal('0.00')
    assert row['updated_at'] == datetime(2026, 10, 1, 9, 0, 0)


def test_standardize_sales_preserves_failed_timestamp_parse(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-03.csv')
    )
    standardized_df = standardize_sales(raw_df)

    row = (
        standardized_df
        .filter(F.col('order_id') == 'O1017')
        .first()
    )

    assert row['raw_updated_at'] == 'not-a-timestamp'
    assert row['updated_at'] is None


def test_standardize_products_parses_boolean(spark):
    raw_df = read_raw_products(
        spark,
        str(TEST_INPUT / 'products.csv')
    )
    standardized_df = standardize_products(raw_df)

    row = (
        standardized_df
        .filter(F.col('product_id') == 'P010')
        .first()
    )

    assert row['raw_active'] == 'false'
    assert row['active'] is False


def test_standardize_stores_parses_boolean(spark):
    raw_df = read_raw_stores(
        spark,
        str(TEST_INPUT / 'stores.csv')
    )
    standardized_df = standardize_stores(raw_df)

    row = (
        standardized_df
        .filter(F.col('store_id') == 'S005')
        .first()
    )

    assert row['raw_active'] == 'false'
    assert row['active'] is False
    assert row['province'] == 'NS'
