"""
Tests for P001 sales business transformations.
"""
from decimal import Decimal
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from p001_retail_sales.transformations import add_sales_metrics


SALES_TEST_SCHEMA = StructType([
    StructField('order_id', StringType(), nullable=False),
    StructField('line_id', IntegerType(), nullable=False),
    StructField('quantity', IntegerType(), nullable=False),
    StructField('unit_price', DecimalType(12, 2), nullable=False),
    StructField('unit_cost', DecimalType(12, 2), nullable=False),
    StructField('discount_amount', DecimalType(12, 2), nullable=False),
    StructField('order_status', StringType(), nullable=False),
])


def test_sales_metrics_for_completed_sale(spark):
    df = spark.createDataFrame(
        [
            (
                'O2001',
                1,
                2,
                Decimal('5.00'),
                Decimal('3.00'),
                Decimal('0.00'),
                'COMPLETED',
            ),
        ],
        schema=SALES_TEST_SCHEMA,
    )

    row = add_sales_metrics(df).first()

    assert row['gross_sales'] == Decimal('10.00')
    assert row['net_sales'] == Decimal('10.00')
    assert row['gross_margin'] == Decimal('4.00')


def test_sales_metrics_apply_discount(spark):
    df = spark.createDataFrame(
        [
            (
                'O2002',
                1,
                2,
                Decimal('10.00'),
                Decimal('6.00'),
                Decimal('3.00'),
                'COMPLETED',
            ),
        ],
        schema=SALES_TEST_SCHEMA,
    )

    row = add_sales_metrics(df).first()

    assert row['gross_sales'] == Decimal('20.00')
    assert row['net_sales'] == Decimal('17.00')
    assert row['gross_margin'] == Decimal('5.00')


def test_sales_metrics_for_return(spark):
    df = spark.createDataFrame(
        [
            (
                'O2003',
                1,
                -1,
                Decimal('25.00'),
                Decimal('15.00'),
                Decimal('0.00'),
                'RETURNED',
            ),
        ],
        schema=SALES_TEST_SCHEMA,
    )

    row = add_sales_metrics(df).first()

    assert row['gross_sales'] == Decimal('-25.00')
    assert row['net_sales'] == Decimal('-25.00')
    assert row['gross_margin'] == Decimal('-10.00')


def test_sales_metrics_for_cancelled_sale_are_zero(spark):
    df = spark.createDataFrame(
        [
            (
                'O2004',
                1,
                3,
                Decimal('20.00'),
                Decimal('12.00'),
                Decimal('5.00'),
                'CANCELLED',
            ),
        ],
        schema=SALES_TEST_SCHEMA,
    )

    row = add_sales_metrics(df).first()

    assert row['gross_sales'] == Decimal('0.00')
    assert row['net_sales'] == Decimal('0.00')
    assert row['gross_margin'] == Decimal('0.00')


def test_sales_metrics_preserve_input_columns(spark):
    df = spark.createDataFrame(
        [
            (
                'O2005',
                2,
                1,
                Decimal('8.00'),
                Decimal('5.00'),
                Decimal('1.00'),
                'COMPLETED',
            ),
        ],
        schema=SALES_TEST_SCHEMA,
    )

    row = add_sales_metrics(df).first()

    assert row['order_id'] == 'O2005'
    assert row['line_id'] == 2
    assert row['quantity'] == 1
    assert row['unit_price'] == Decimal('8.00')
    assert row['unit_cost'] == Decimal('5.00')
    assert row['discount_amount'] == Decimal('1.00')
    assert row['order_status'] == 'COMPLETED'


def test_sales_metric_columns_use_expected_decimal_type(spark):
    df = spark.createDataFrame(
        [
            (
                'O2006',
                1,
                1,
                Decimal('10.00'),
                Decimal('6.00'),
                Decimal('0.00'),
                'COMPLETED',
            ),
        ],
        schema=SALES_TEST_SCHEMA,
    )

    transformed_df = add_sales_metrics(df)

    assert transformed_df.schema['gross_sales'].dataType == DecimalType(24, 2)
    assert transformed_df.schema['net_sales'].dataType == DecimalType(24, 2)
    assert transformed_df.schema['gross_margin'].dataType == DecimalType(24, 2)
