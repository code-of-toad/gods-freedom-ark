"""
Tests for P001 analytical SQL.
"""

from datetime import date, datetime
from decimal import Decimal

import pytest

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from p001_retail_sales.analytics import (
    AnalyticsQueryError,
    load_analytics_queries,
    register_analytical_views,
    run_analytics_query,
)
from p001_retail_sales.modeling import (
    build_dim_date,
)


FACT_SCHEMA = StructType([
    StructField(
        'order_id',
        StringType(),
        nullable=False,
    ),
    StructField(
        'line_id',
        IntegerType(),
        nullable=False,
    ),
    StructField(
        'sale_date',
        DateType(),
        nullable=False,
    ),
    StructField(
        'store_id',
        StringType(),
        nullable=False,
    ),
    StructField(
        'product_id',
        StringType(),
        nullable=False,
    ),
    StructField(
        'quantity',
        IntegerType(),
        nullable=False,
    ),
    StructField(
        'unit_price',
        DecimalType(12, 2),
        nullable=False,
    ),
    StructField(
        'unit_cost',
        DecimalType(12, 2),
        nullable=False,
    ),
    StructField(
        'discount_amount',
        DecimalType(12, 2),
        nullable=False,
    ),
    StructField(
        'order_status',
        StringType(),
        nullable=False,
    ),
    StructField(
        'updated_at',
        TimestampType(),
        nullable=False,
    ),
    StructField(
        'gross_sales',
        DecimalType(24, 2),
        nullable=False,
    ),
    StructField(
        'net_sales',
        DecimalType(24, 2),
        nullable=False,
    ),
    StructField(
        'gross_margin',
        DecimalType(24, 2),
        nullable=False,
    ),
])


PRODUCT_SCHEMA = StructType([
    StructField(
        'product_id',
        StringType(),
        nullable=False,
    ),
    StructField(
        'product_name',
        StringType(),
        nullable=False,
    ),
    StructField(
        'category',
        StringType(),
        nullable=False,
    ),
    StructField(
        'active',
        BooleanType(),
        nullable=False,
    ),
])


STORE_SCHEMA = StructType([
    StructField(
        'store_id',
        StringType(),
        nullable=False,
    ),
    StructField(
        'store_name',
        StringType(),
        nullable=False,
    ),
    StructField(
        'city',
        StringType(),
        nullable=False,
    ),
    StructField(
        'province',
        StringType(),
        nullable=False,
    ),
    StructField(
        'active',
        BooleanType(),
        nullable=False,
    ),
])


def _register_test_model(spark):
    fact_sales_df = spark.createDataFrame(
        [
            (
                'O1',
                1,
                date(2026, 10, 1),
                'S1',
                'P1',
                2,
                Decimal('5.00'),
                Decimal('3.00'),
                Decimal('0.00'),
                'COMPLETED',
                datetime(2026, 10, 1, 9, 0),
                Decimal('10.00'),
                Decimal('10.00'),
                Decimal('4.00'),
            ),
            (
                'O1',
                2,
                date(2026, 10, 1),
                'S1',
                'P2',
                1,
                Decimal('5.00'),
                Decimal('3.00'),
                Decimal('0.00'),
                'COMPLETED',
                datetime(2026, 10, 1, 9, 0),
                Decimal('5.00'),
                Decimal('5.00'),
                Decimal('2.00'),
            ),
            (
                'O2',
                1,
                date(2026, 10, 1),
                'S2',
                'P3',
                1,
                Decimal('20.00'),
                Decimal('12.00'),
                Decimal('0.00'),
                'COMPLETED',
                datetime(2026, 10, 1, 10, 0),
                Decimal('20.00'),
                Decimal('20.00'),
                Decimal('8.00'),
            ),
            (
                'O3',
                1,
                date(2026, 10, 2),
                'S1',
                'P1',
                -1,
                Decimal('5.00'),
                Decimal('3.00'),
                Decimal('0.00'),
                'RETURNED',
                datetime(2026, 10, 2, 10, 0),
                Decimal('-5.00'),
                Decimal('-5.00'),
                Decimal('-2.00'),
            ),
        ],
        schema=FACT_SCHEMA,
    )

    dim_product_df = spark.createDataFrame(
        [
            (
                'P1',
                'Coffee',
                'Grocery',
                True,
            ),
            (
                'P2',
                'Tea',
                'Grocery',
                True,
            ),
            (
                'P3',
                'Headphones',
                'Electronics',
                True,
            ),
        ],
        schema=PRODUCT_SCHEMA,
    )

    dim_store_df = spark.createDataFrame(
        [
            (
                'S1',
                'Toronto Central',
                'Toronto',
                'ON',
                True,
            ),
            (
                'S2',
                'Vancouver West',
                'Vancouver',
                'BC',
                True,
            ),
        ],
        schema=STORE_SCHEMA,
    )

    dim_date_df = build_dim_date(
        fact_sales_df
    )

    register_analytical_views(
        fact_sales_df=fact_sales_df,
        dim_product_df=dim_product_df,
        dim_store_df=dim_store_df,
        dim_date_df=dim_date_df,
    )


def test_analytics_sql_contains_expected_queries():
    queries = load_analytics_queries()

    assert set(queries) == {
        'daily_sales',
        'store_performance',
        'product_performance',
        'category_performance',
        'province_performance',
        'return_activity',
        'order_value_summary',
    }


def test_daily_sales_query(spark):
    _register_test_model(
        spark
    )

    rows = (
        run_analytics_query(
            spark,
            'daily_sales',
        )
        .collect()
    )

    assert len(rows) == 2

    first = rows[0]

    assert first['sale_date'] == date(2026, 10, 1)
    assert first['gross_sales'] == Decimal('35.00')
    assert first['net_sales'] == Decimal('35.00')
    assert first['gross_margin'] == Decimal('14.00')
    assert first['net_units'] == 4
    assert first['order_count'] == 2
    assert first['line_count'] == 3

    second = rows[1]

    assert second['sale_date'] == date(2026, 10, 2)
    assert second['net_sales'] == Decimal('-5.00')
    assert second['gross_margin'] == Decimal('-2.00')
    assert second['net_units'] == -1


def test_category_performance_query(spark):
    _register_test_model(
        spark
    )

    rows = (
        run_analytics_query(
            spark,
            'category_performance',
        )
        .collect()
    )

    by_category = {
        row['category']: row
        for row in rows
    }

    grocery = by_category[
        'Grocery'
    ]

    electronics = by_category[
        'Electronics'
    ]

    # Grocery:
    #
    # 10 + 5 - 5 = 10 net sales
    # 4 + 2 - 2 = 4 gross margin
    assert grocery['net_sales'] == Decimal('10.00')
    assert grocery['gross_margin'] == Decimal('4.00')
    assert grocery['net_units'] == 2

    assert electronics['net_sales'] == Decimal('20.00')
    assert electronics['gross_margin'] == Decimal('8.00')


def test_return_activity_query(spark):
    _register_test_model(
        spark
    )

    row = (
        run_analytics_query(
            spark,
            'return_activity',
        )
        .first()
    )

    assert row['sale_date'] == date(2026, 10, 2)
    assert row['return_line_count'] == 1
    assert row['return_order_count'] == 1
    assert row['returned_units'] == 1
    assert row['return_value'] == Decimal('5.00')
    assert row['margin_reversal'] == Decimal('2.00')


def test_order_value_summary_query(spark):
    _register_test_model(
        spark
    )

    row = (
        run_analytics_query(
            spark,
            'order_value_summary',
        )
        .filter(
            'sale_date = DATE "2026-10-01"'
        )
        .first()
    )

    # Completed order totals:
    #
    # O1 = 10 + 5 = 15
    # O2 = 20
    #
    # average = (15 + 20) / 2 = 17.50
    assert row['completed_order_count'] == 2
    assert row['completed_net_sales'] == Decimal('35.00')
    assert row['average_order_value'] == Decimal('17.50')


def test_unknown_analytics_query_fails(spark):
    _register_test_model(
        spark
    )

    with pytest.raises(
        AnalyticsQueryError,
        match='Unknown analytical query',
    ):
        run_analytics_query(
            spark,
            'customer_lifetime_value',
        )
