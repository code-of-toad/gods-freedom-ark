"""
Tests for raw P001 ingestion.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from p001_retail_sales.ingestion import (
    read_raw_sales,
    read_raw_products,
    read_raw_stores,
)

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]  # ==> P001-retail-sales/
TEST_INPUT = PROJECT_ROOT / 'data' / 'test' / 'input'


def test_raw_sales_preserves_invalid_numeric_text(spark):
    sales_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv')
    )
    row = (
        sales_df
        .filter(F.col('order_id') == 'O1007')
        .first()
    )

    assert row['quantity'] == 'five'
    assert isinstance(
        sales_df.schema['quantity'].dataType,
        StringType,
    )


def test_raw_products_load_expected_rows(spark):
    proudcts_df = read_raw_products(
        spark,
        str(TEST_INPUT / 'products.csv'),
    )

    assert proudcts_df.count() == 10


def test_raw_stores_load_expected_rows(spark):
    stores_df = read_raw_stores(
        spark,
        str(TEST_INPUT / 'stores.csv'),
    )

    assert stores_df.count() == 5
