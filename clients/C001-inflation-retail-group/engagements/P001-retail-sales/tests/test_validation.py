"""
Tests for P001 validation behaviour.

Test cases will be added with validation implementation.
"""
from pathlib import Path
from pyspark.sql import functions as F

from p001_retail_sales.schemas import (
    RAW_PRODUCTS_SCHEMA,
    RAW_STORES_SCHEMA,
)
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
from p001_retail_sales.validation import (
    add_sales_rejection_reasons,
    add_products_rejection_reasons,
    add_stores_rejection_reasons,
    validate_sales,
    split_validated_records,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_INPUT = PROJECT_ROOT / 'data' / 'test' / 'input'


# =============================================================================
# SALES ROW-LEVEL DATA VALIDATION
# =============================================================================


def _get_validated_reference_data(spark):
    raw_products_df = read_raw_products(
        spark,
        str(TEST_INPUT / 'products.csv'),
    )
    products_df = add_products_rejection_reasons(
        standardize_products(raw_products_df)
    )

    raw_stores_df = read_raw_stores(
        spark,
        str(TEST_INPUT / 'stores.csv'),
    )
    stores_df = add_stores_rejection_reasons(
        standardize_stores(raw_stores_df)
    )

    return products_df, stores_df


def test_sales_validation_accepts_valid_sale(spark):
    raw_df_1 = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv')
    )
    validated_df_1 = add_sales_rejection_reasons(standardize_sales(raw_df_1))
    row_1 = (
        validated_df_1
        .filter(
            (F.col('order_id') == 'O1001')
            & (F.col('line_id') == 1)
        )
        .first()
    )
    assert row_1['rejection_reasons'] == []

    raw_df_2 = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-02.csv')
    )
    validated_df_2 = add_sales_rejection_reasons(standardize_sales(raw_df_2))
    row_2 = (
        validated_df_2
        .filter(
            (F.col('order_id') == 'O1010')
            & (F.col('line_id') == 1)
        )
        .first()
    )
    assert row_2['rejection_reasons'] == []


def test_sales_validation_rejects_invalid_quantity(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv')
    )

    validated_df = add_sales_rejection_reasons(standardize_sales(raw_df))
    row_1 = (
        validated_df
        .filter(F.col('order_id') == 'O1005')
        .first()
    )
    row_2 = (
        validated_df
        .filter(F.col('order_id') == 'O1007')
        .first()
    )

    assert 'INVALID_QUANTITY' in row_1['rejection_reasons']
    assert 'INVALID_QUANTITY' in row_2['rejection_reasons']


def test_sales_validation_rejects_invalid_unit_price(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv')
    )

    validated_df = add_sales_rejection_reasons(standardize_sales(raw_df))
    row = (
        validated_df
        .filter(F.col('order_id') == 'O1006')
        .first()
    )

    assert 'INVALID_UNIT_PRICE' in row['rejection_reasons']


def test_sales_validation_rejects_invalid_order_status(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-02.csv')
    )

    validated_df = add_sales_rejection_reasons(standardize_sales(raw_df))
    row = (
        validated_df
        .filter(F.col('order_id') == 'O1009')
        .first()
    )

    assert 'INVALID_ORDER_STATUS' in row['rejection_reasons']


def test_sales_validation_rejects_discount_exceeding_gross_sales(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-02.csv')
    )

    validated_df = add_sales_rejection_reasons(standardize_sales(raw_df))
    row = (
        validated_df
        .filter(F.col('order_id') == 'O1012')
        .first()
    )

    assert 'DISCOUNT_EXCEEDS_GROSS_SALES' in row['rejection_reasons']


def test_sales_validation_rejects_invalid_updated_at(spark):
    raw_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-03.csv')
    )

    validated_df = add_sales_rejection_reasons(standardize_sales(raw_df))
    row = (
        validated_df
        .filter(F.col('order_id') == 'O1017')
        .first()
    )

    assert 'INVALID_UPDATED_AT' in row['rejection_reasons']


# -----------------------------------------------------------------------------
# PRODUCT REFERENCE-DATA VALIDATION
# -----------------------------------------------------------------------------


def test_products_validation_accepts_valid_product(spark):
    raw_df = read_raw_products(
        spark,
        str(TEST_INPUT / 'products.csv')
    )

    validated_df = add_products_rejection_reasons(
        standardize_products(raw_df)
    )

    row = (
        validated_df
        .filter(F.col('product_id') == 'P001')
        .first()
    )

    assert row['rejection_reasons'] == []


def test_products_validation_rejects_missing_product_id(spark):
    raw_df = spark.createDataFrame(
        [
            ('', 'Coffee', 'Grocery', 'true'),
        ],
        schema=RAW_PRODUCTS_SCHEMA,
    )

    validated_df = add_products_rejection_reasons(
        standardize_products(raw_df)
    )

    row = validated_df.first()

    assert 'MISSING_PRODUCT_ID' in row['rejection_reasons']


def test_products_validation_rejects_missing_product_name(spark):
    raw_df = spark.createDataFrame(
        [
            ('P001', '', 'Grocery', 'true'),
        ],
        schema=RAW_PRODUCTS_SCHEMA,
    )

    validated_df = add_products_rejection_reasons(
        standardize_products(raw_df)
    )

    row = validated_df.first()

    assert 'MISSING_PRODUCT_NAME' in row['rejection_reasons']


def test_products_validation_rejects_missing_category(spark):
    raw_df = spark.createDataFrame(
        [
            ('P001', 'Coffee', '', 'true'),
        ],
        schema=RAW_PRODUCTS_SCHEMA,
    )

    validated_df = add_products_rejection_reasons(
        standardize_products(raw_df)
    )

    row = validated_df.first()

    assert 'MISSING_CATEGORY' in row['rejection_reasons']


def test_products_validation_rejects_invalid_active(spark):
    raw_df = spark.createDataFrame(
        [
            ('P001', 'Coffee', 'Grocery', 'banana'),
        ],
        schema=RAW_PRODUCTS_SCHEMA,
    )

    validated_df = add_products_rejection_reasons(
        standardize_products(raw_df)
    )

    row = validated_df.first()

    assert row['active'] is None
    assert 'INVALID_ACTIVE' in row['rejection_reasons']


def test_products_validation_marks_all_duplicate_ids(spark):
    raw_df = spark.createDataFrame(
        [
            ('P001', 'Coffee', 'Grocery', 'true'),
            ('P001', 'Coffee Duplicate', 'Grocery', 'true'),
        ],
        schema=RAW_PRODUCTS_SCHEMA,
    )

    validated_df = add_products_rejection_reasons(
        standardize_products(raw_df)
    )

    rows = validated_df.collect()

    assert len(rows) == 2

    for row in rows:
        assert 'DUPLICATE_PRODUCT_ID' in row['rejection_reasons']


# -----------------------------------------------------------------------------
# STORE REFERENCE-DATA VALIDATION
# -----------------------------------------------------------------------------


def test_stores_validation_accepts_valid_store(spark):
    raw_df = read_raw_stores(
        spark,
        str(TEST_INPUT / 'stores.csv')
    )

    validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_df)
    )

    row = (
        validated_df
        .filter(F.col('store_id') == 'S001')
        .first()
    )

    assert row['rejection_reasons'] == []


def test_stores_validation_rejects_missing_store_id(spark):
    raw_df = spark.createDataFrame(
        [
            ('', 'Mississauga Central', 'Mississauga', 'ON', 'true'),
        ],
        schema=RAW_STORES_SCHEMA,
    )

    validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_df)
    )

    row = validated_df.first()

    assert 'MISSING_STORE_ID' in row['rejection_reasons']


def test_stores_validation_rejects_missing_store_name(spark):
    raw_df = spark.createDataFrame(
        [
            ('S001', '', 'Mississauga', 'ON', 'true'),
        ],
        schema=RAW_STORES_SCHEMA,
    )

    validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_df)
    )

    row = validated_df.first()

    assert 'MISSING_STORE_NAME' in row['rejection_reasons']


def test_stores_validation_rejects_missing_city(spark):
    raw_df = spark.createDataFrame(
        [
            ('S001', 'Mississauga Central', '', 'ON', 'true'),
        ],
        schema=RAW_STORES_SCHEMA,
    )

    validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_df)
    )

    row = validated_df.first()

    assert 'MISSING_CITY' in row['rejection_reasons']


def test_stores_validation_rejects_missing_province(spark):
    raw_df = spark.createDataFrame(
        [
            ('S001', 'Mississauga Central', 'Mississauga', '', 'true'),
        ],
        schema=RAW_STORES_SCHEMA,
    )

    validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_df)
    )

    row = validated_df.first()

    assert 'MISSING_PROVINCE' in row['rejection_reasons']


def test_stores_validation_rejects_invalid_province(spark):
    raw_df = spark.createDataFrame(
        [
            ('S001', 'Mississauga Central', 'Mississauga', 'XX', 'true'),
        ],
        schema=RAW_STORES_SCHEMA,
    )

    validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_df)
    )

    row = validated_df.first()

    assert 'INVALID_PROVINCE' in row['rejection_reasons']


def test_stores_validation_rejects_invalid_active(spark):
    raw_df = spark.createDataFrame(
        [
            ('S001', 'Mississauga Central', 'Mississauga', 'ON', 'banana'),
        ],
        schema=RAW_STORES_SCHEMA,
    )

    validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_df)
    )

    row = validated_df.first()

    assert row['active'] is None
    assert 'INVALID_ACTIVE' in row['rejection_reasons']


def test_stores_validation_marks_all_duplicate_ids(spark):
    raw_df = spark.createDataFrame(
        [
            ('S001', 'Mississauga Central', 'Mississauga', 'ON', 'true'),
            ('S001', 'Mississauga Duplicate', 'Mississauga', 'ON', 'true'),
        ],
        schema=RAW_STORES_SCHEMA,
    )

    validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_df)
    )

    rows = validated_df.collect()

    assert len(rows) == 2

    for row in rows:
        assert 'DUPLICATE_STORE_ID' in row['rejection_reasons']


# -----------------------------------------------------------------------------
# REFERENTIAL-INTEGRITY CHECKS
# -----------------------------------------------------------------------------


def test_sales_reference_validation_accepts_known_references(spark):
    products_df, stores_df = _get_validated_reference_data(spark)

    raw_sales_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv'),
    )

    validated_df = validate_sales(
        standardize_sales(raw_sales_df),
        products_df,
        stores_df,
    )

    row = (
        validated_df
        .filter(
            (F.col('order_id') == 'O1001')
            & (F.col('line_id') == 1)
        )
        .first()
    )

    assert 'UNKNOWN_PRODUCT_ID' not in row['rejection_reasons']
    assert 'UNKNOWN_STORE_ID' not in row['rejection_reasons']


def test_sales_reference_validation_rejects_unknown_product(spark):
    products_df, stores_df = _get_validated_reference_data(spark)

    raw_sales_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv'),
    )

    validated_df = validate_sales(
        standardize_sales(raw_sales_df),
        products_df,
        stores_df,
    )

    row = (
        validated_df
        .filter(F.col('order_id') == 'O1003')
        .first()
    )

    assert 'UNKNOWN_PRODUCT_ID' in row['rejection_reasons']


def test_sales_reference_validation_rejects_unknown_store(spark):
    products_df, stores_df = _get_validated_reference_data(spark)

    raw_sales_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv'),
    )

    validated_df = validate_sales(
        standardize_sales(raw_sales_df),
        products_df,
        stores_df,
    )

    row = (
        validated_df
        .filter(F.col('order_id') == 'O1004')
        .first()
    )

    assert 'UNKNOWN_STORE_ID' in row['rejection_reasons']


def test_sales_reference_validation_rejects_invalid_reference_row(spark):
    # Start with the normal product reference data.
    raw_products_df = read_raw_products(
        spark,
        str(TEST_INPUT / 'products.csv'),
    )

    # P999 physically exists, but its active value cannot be standardized
    # into a valid boolean.
    invalid_product_df = spark.createDataFrame(
        [
            ('P999', 'Invalid Product', 'Grocery', 'banana'),
        ],
        schema=RAW_PRODUCTS_SCHEMA,
    )

    raw_products_df = raw_products_df.unionByName(
        invalid_product_df
    )

    products_df = add_products_rejection_reasons(
        standardize_products(raw_products_df)
    )

    raw_stores_df = read_raw_stores(
        spark,
        str(TEST_INPUT / 'stores.csv'),
    )

    stores_df = add_stores_rejection_reasons(
        standardize_stores(raw_stores_df)
    )

    # Confirm that P999 physically exists but is itself invalid.
    product_row = (
        products_df
        .filter(F.col('product_id') == 'P999')
        .first()
    )

    assert 'INVALID_ACTIVE' in product_row['rejection_reasons']

    raw_sales_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv'),
    )

    validated_df = validate_sales(
        standardize_sales(raw_sales_df),
        products_df,
        stores_df,
    )

    row = (
        validated_df
        .filter(F.col('order_id') == 'O1003')
        .first()
    )

    # The ID exists physically, but not in the trusted product set.
    assert 'UNKNOWN_PRODUCT_ID' in row['rejection_reasons']


# -----------------------------------------------------------------------------
# REJECTED-DATA QUARANTINE
# -----------------------------------------------------------------------------


def test_split_validated_records_classifies_sales_correctly(spark):
    products_df, stores_df = _get_validated_reference_data(spark)

    raw_sales_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv'),
    )

    validated_df = validate_sales(
        standardize_sales(raw_sales_df),
        products_df,
        stores_df,
    )

    accepted_df, quarantined_df = split_validated_records(
        validated_df
    )

    # Valid sale should be accepted.
    accepted_o1001 = (
        accepted_df
        .filter(
            (F.col('order_id') == 'O1001')
            & (F.col('line_id') == 1)
        )
        .count()
    )

    assert accepted_o1001 == 1

    # Unknown product should be quarantined.
    quarantined_o1003 = (
        quarantined_df
        .filter(F.col('order_id') == 'O1003')
        .count()
    )

    assert quarantined_o1003 == 1

    # Invalid quantity should be quarantined.
    quarantined_o1005 = (
        quarantined_df
        .filter(F.col('order_id') == 'O1005')
        .count()
    )

    assert quarantined_o1005 == 1

    # Invalid unit price should be quarantined.
    quarantined_o1006 = (
        quarantined_df
        .filter(F.col('order_id') == 'O1006')
        .count()
    )

    assert quarantined_o1006 == 1


def test_split_validated_records_preserves_every_row(spark):
    products_df, stores_df = _get_validated_reference_data(spark)

    raw_sales_df = read_raw_sales(
        spark,
        str(TEST_INPUT / 'sales' / 'sales_2026-10-01.csv'),
    )

    validated_df = validate_sales(
        standardize_sales(raw_sales_df),
        products_df,
        stores_df,
    )

    accepted_df, quarantined_df = split_validated_records(
        validated_df
    )

    assert (
        accepted_df.count()
        + quarantined_df.count()
        == validated_df.count()
    )
