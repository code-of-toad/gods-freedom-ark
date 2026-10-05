"""
Tests for the P001 analytical model.
"""
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from p001_retail_sales.modeling import (
    DIM_DATE_COLUMNS,
    DIM_PRODUCT_COLUMNS,
    DIM_STORE_COLUMNS,
    FACT_SALES_COLUMNS,
    build_dim_date,
    build_dim_product,
    build_dim_store,
    build_fact_sales,
)


FACT_INPUT_SCHEMA = StructType([
    StructField(
        'raw_order_id',
        StringType(),
        nullable=True,
    ),
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
        'rejection_reasons',
        ArrayType(
            StringType(),
            containsNull=False,
        ),
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


PRODUCT_INPUT_SCHEMA = StructType([
    StructField(
        'raw_product_id',
        StringType(),
        nullable=True,
    ),
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
    StructField(
        'rejection_reasons',
        ArrayType(
            StringType(),
            containsNull=False,
        ),
        nullable=False,
    ),
])


STORE_INPUT_SCHEMA = StructType([
    StructField(
        'raw_store_id',
        StringType(),
        nullable=True,
    ),
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
    StructField(
        'rejection_reasons',
        ArrayType(
            StringType(),
            containsNull=False,
        ),
        nullable=False,
    ),
])


def _fact_input_df(spark):
    return spark.createDataFrame(
        [
            (
                ' O5001 ',
                'O5001',
                1,
                date(2026, 10, 4),
                'S001',
                'P001',
                2,
                Decimal('5.50'),
                Decimal('3.00'),
                Decimal('0.25'),
                'COMPLETED',
                datetime(
                    2026,
                    10,
                    4,
                    10,
                    0,
                    0,
                ),
                [],
                Decimal('11.00'),
                Decimal('10.75'),
                Decimal('4.75'),
            ),
            (
                ' O5002 ',
                'O5002',
                1,
                date(2026, 10, 5),
                'S002',
                'P002',
                -1,
                Decimal('25.00'),
                Decimal('15.00'),
                Decimal('0.00'),
                'RETURNED',
                datetime(
                    2026,
                    10,
                    5,
                    11,
                    0,
                    0,
                ),
                [],
                Decimal('-25.00'),
                Decimal('-25.00'),
                Decimal('-10.00'),
            ),
        ],
        schema=FACT_INPUT_SCHEMA,
    )


def test_build_fact_sales_has_expected_columns(
    spark,
):
    candidate_df = _fact_input_df(
        spark
    )

    fact_df = build_fact_sales(
        candidate_df
    )

    assert fact_df.columns == FACT_SALES_COLUMNS


def test_build_fact_sales_excludes_processing_columns(
    spark,
):
    candidate_df = _fact_input_df(
        spark
    )

    fact_df = build_fact_sales(
        candidate_df
    )

    assert 'raw_order_id' not in fact_df.columns
    assert 'rejection_reasons' not in fact_df.columns

    assert fact_df.count() == 2


def test_build_fact_sales_preserves_metrics(
    spark,
):
    candidate_df = _fact_input_df(
        spark
    )

    row = (
        build_fact_sales(candidate_df)
        .filter(
            'order_id = "O5001"'
        )
        .first()
    )

    assert row['gross_sales'] == Decimal('11.00')
    assert row['net_sales'] == Decimal('10.75')
    assert row['gross_margin'] == Decimal('4.75')


def test_build_dim_product_selects_analytical_columns(
    spark,
):
    products_df = spark.createDataFrame(
        [
            (
                'P001',
                'P001',
                'Coffee',
                'Grocery',
                True,
                [],
            ),
            (
                'P010',
                'P010',
                'Old Product',
                'Legacy',
                False,
                [],
            ),
        ],
        schema=PRODUCT_INPUT_SCHEMA,
    )

    dim_df = build_dim_product(
        products_df
    )

    assert dim_df.columns == DIM_PRODUCT_COLUMNS
    assert dim_df.count() == 2

    inactive = (
        dim_df
        .filter(
            'product_id = "P010"'
        )
        .first()
    )

    assert inactive['active'] is False


def test_build_dim_store_selects_analytical_columns(
    spark,
):
    stores_df = spark.createDataFrame(
        [
            (
                'S001',
                'S001',
                'Downtown',
                'Toronto',
                'ON',
                True,
                [],
            ),
            (
                'S005',
                'S005',
                'Old Store',
                'Ottawa',
                'ON',
                False,
                [],
            ),
        ],
        schema=STORE_INPUT_SCHEMA,
    )

    dim_df = build_dim_store(
        stores_df
    )

    assert dim_df.columns == DIM_STORE_COLUMNS
    assert dim_df.count() == 2

    inactive = (
        dim_df
        .filter(
            'store_id = "S005"'
        )
        .first()
    )

    assert inactive['active'] is False


def test_build_dim_date_has_one_row_per_business_date(
    spark,
):
    fact_df = _fact_input_df(
        spark
    )

    dim_df = build_dim_date(
        fact_df
    )

    assert dim_df.columns == DIM_DATE_COLUMNS
    assert dim_df.count() == 2

    dates = {
        row['sale_date']
        for row in dim_df.collect()
    }

    assert dates == {
        date(2026, 10, 4),
        date(2026, 10, 5),
    }


def test_build_dim_date_derives_calendar_attributes(
    spark,
):
    fact_df = _fact_input_df(
        spark
    )

    row = (
        build_dim_date(fact_df)
        .filter(
            'sale_date = DATE "2026-10-04"'
        )
        .first()
    )

    # 2026-10-04 is a Sunday.
    assert row['year'] == 2026
    assert row['quarter'] == 4
    assert row['month'] == 10
    assert row['month_name'] == 'October'
    assert row['day_of_month'] == 4
    assert row['day_of_week'] == 1
    assert row['day_name'] == 'Sunday'
    assert row['is_weekend'] is True


def test_build_dim_date_marks_weekday_correctly(
    spark,
):
    fact_df = _fact_input_df(
        spark
    )

    row = (
        build_dim_date(fact_df)
        .filter(
            'sale_date = DATE "2026-10-05"'
        )
        .first()
    )

    # 2026-10-05 is a Monday.
    assert row['day_of_week'] == 2
    assert row['day_name'] == 'Monday'
    assert row['is_weekend'] is False


def test_build_dim_date_removes_duplicate_dates(
    spark,
):
    fact_df = _fact_input_df(
        spark
    )

    duplicated_df = fact_df.unionByName(
        fact_df.filter(
            'order_id = "O5001"'
        )
    )

    dim_df = build_dim_date(
        duplicated_df
    )

    assert dim_df.count() == 2
