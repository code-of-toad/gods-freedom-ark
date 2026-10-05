"""
Analytical model builders for P001 retail sales.

These functions operate only on already-trusted P001 DataFrames.

They do not perform:
- ingestion;
- validation;
- quarantine;
- version resolution; or
- incremental processing.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


FACT_SALES_COLUMNS = [
    'order_id',
    'line_id',
    'sale_date',
    'store_id',
    'product_id',
    'quantity',
    'unit_price',
    'unit_cost',
    'discount_amount',
    'order_status',
    'updated_at',
    'gross_sales',
    'net_sales',
    'gross_margin',
]

DIM_PRODUCT_COLUMNS = [
    'product_id',
    'product_name',
    'category',
    'active',
]

DIM_STORE_COLUMNS = [
    'store_id',
    'store_name',
    'city',
    'province',
    'active',
]

DIM_DATE_COLUMNS = [
    'sale_date',
    'year',
    'quarter',
    'month',
    'month_name',
    'day_of_month',
    'day_of_week',
    'day_name',
    'is_weekend',
]


def build_fact_sales(candidate_df: DataFrame) -> DataFrame:
    """
    Build the current-state analytical sales fact.

    Grain:
        One trusted current row per (order_id, line_id).

    Assumes:
        - validation has completed;
        - version resolution has completed;
        - analytical metrics have been added;
        - reconciliation has passed.

    Processing-only fields such as raw_* and rejection_reasons are
    deliberately excluded from the analytical fact table.
    """
    return candidate_df.select(*FACT_SALES_COLUMNS)


def build_dim_product(products_df: DataFrame) -> DataFrame:
    """
    Build the current canonical product dimension.

    Grain:
        One trusted row per product_id.

    Assumes products_df contains only reference rows that passed
    product validation.
    """
    return products_df.select(*DIM_PRODUCT_COLUMNS)


def build_dim_store(stores_df: DataFrame) -> DataFrame:
    """
    Build the current canonical store dimension.

    Grain:
        One trusted row per store_id.

    Assumes stores_df contains only reference rows that passed
    store validation.
    """
    return stores_df.select(*DIM_STORE_COLUMNS)


def build_dim_date(fact_sales_df: DataFrame) -> DataFrame:
    """
    Build a date dimension from business dates represented in
    trusted current sales.

    Grain:
        One row per distinct sale_date.

    Spark day_of_week semantics:
        1 = Sunday
        2 = Monday
        ...
        7 = Saturday
    """
    return (
        fact_sales_df
        .select('sale_date')
        .distinct()
        .withColumn(
            'year',
            F.year('sale_date'),
        )
        .withColumn(
            'quarter',
            F.quarter('sale_date'),
        )
        .withColumn(
            'month',
            F.month('sale_date'),
        )
        .withColumn(
            'month_name',
            F.date_format('sale_date', 'MMMM'),
        )
        .withColumn(
            'day_of_month',
            F.dayofmonth('sale_date'),
        )
        .withColumn(
            'day_of_week',
            F.dayofweek('sale_date'),
        )
        .withColumn(
            'day_name',
            F.date_format('sale_date', 'EEEE'),
        )
        .withColumn(
            'is_weekend',
            F.dayofweek('sale_date').isin(1, 7)
        )
        .select(*DIM_DATE_COLUMNS)
    )
