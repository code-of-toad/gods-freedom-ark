"""
Standardization for raw P001 datasets.

"What is the canonical representation?"

- Trim whitespace.
- Parse integers/dates/timestamps/decimals.
- Normalize booleans/status casing where appropriate.
- Preserve enough raw information to identify parse failures.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def standardize_sales(raw_df: DataFrame) -> DataFrame:
    """
    Convert raw sales values into canonical representation while preserving
    the original source values.
    """
    raw_columns = [
        F.col(column).alias(f'raw_{column}')
        for column in raw_df.columns
    ]

    return raw_df.select(
        *raw_columns,

        F.trim(
            F.col('order_id')
        ).alias('order_id'),

        F.expr(
            'try_cast(trim(line_id) AS INT)'
        ).alias('line_id'),

        F.expr(
            'try_cast(trim(sale_date) AS DATE)'
        ).alias('sale_date'),

        F.trim(
            F.col('store_id')
        ).alias('store_id'),

        F.trim(
            F.col('product_id')
        ).alias('product_id'),

        F.expr(
            'try_cast(trim(quantity) AS INT)'
        ).alias('quantity'),

        F.expr(
            'try_cast(trim(unit_price) AS DECIMAL(12, 2))'
        ).alias('unit_price'),

        F.expr(
            'try_cast(trim(unit_cost) AS DECIMAL(12, 2))'
        ).alias('unit_cost'),

        F.expr(
            'try_cast(trim(discount_amount) AS DECIMAL(12, 2))'
        ).alias('discount_amount'),

        F.upper(
            F.trim(F.col('order_status'))
        ).alias('order_status'),

        F.expr(
            'try_cast(trim(updated_at) AS TIMESTAMP)'
        ).alias('updated_at'),
    )


def standardize_products(raw_df: DataFrame) -> DataFrame:
    raw_columns = [
        F.col(column).alias(f'raw_{column}')
        for column in raw_df.columns
    ]

    return raw_df.select(
        *raw_columns,

        F.trim(
            F.col('product_id')
        ).alias('product_id'),

        F.trim(
            F.col('product_name')
        ).alias('product_name'),

        F.trim(
            F.col('category')
        ).alias('category'),

        F.when(
            F.lower(F.trim(F.col('active'))) == 'true',
            F.lit(True),
        ).when(
            F.lower(F.trim(F.col('active'))) == 'false',
            F.lit(False),
        ).otherwise(
            F.lit(None).cast('boolean')
        ).alias('active'),
    )


def standardize_stores(raw_df: DataFrame) -> DataFrame:
    raw_columns = [
        F.col(column).alias(f'raw_{column}')
        for column in raw_df.columns
    ]

    return raw_df.select(
        *raw_columns,

        F.trim(
            F.col('store_id')
        ).alias('store_id'),

        F.trim(
            F.col('store_name')
        ).alias('store_name'),

        F.trim(
            F.col('city')
        ).alias('city'),

        F.upper(
            F.trim(F.col('province'))
        ).alias('province'),

        F.when(
            F.lower(F.trim(F.col('active'))) == 'true',
            F.lit(True)
        ).when(
            F.lower(F.trim(F.col('active'))) == 'false',
            F.lit(False)
        ).otherwise(
            F.lit(None).cast('boolean')
        ).alias('active'),
    )
