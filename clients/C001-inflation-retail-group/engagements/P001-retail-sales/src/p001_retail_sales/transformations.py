"""
Deterministic business transformations for P001 sales.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SALES_METRIC_TYPE = 'decimal(24,2)'


def add_sales_metrics(df: DataFrame) -> DataFrame:
    """
    Add analytical sales measures to resolved sales records.

    Assumes:
        - rows have passed validation
        - duplicate/version resolution has completed
        - each row represents authoritative current state
    """
    zero = F.lit(0).cast(SALES_METRIC_TYPE)

    gross_sales = (
        F.col('quantity')
        * F.col('unit_price')
    ).cast(SALES_METRIC_TYPE)

    line_cost = (
        F.col('quantity')
        * F.col('unit_cost')
    ).cast(SALES_METRIC_TYPE)

    return (
        df
        .withColumn(
            'gross_sales',
            F.when(
                F.col('order_status') == 'CANCELLED',
                zero,
            ).otherwise(
                gross_sales
            ),
        )
        .withColumn(
            'net_sales',
            F.when(
                F.col('order_status') == 'CANCELLED',
                zero,
            ).otherwise(
                (
                    F.col('gross_sales')
                    - F.col('discount_amount')
                ).cast(SALES_METRIC_TYPE)
            ),
        )
        .withColumn(
            'gross_margin',
            F.when(
                F.col('order_status') == 'CANCELLED',
                zero,
            ).otherwise(
                (
                    F.col('net_sales')
                    - line_cost
                ).cast(SALES_METRIC_TYPE)
            ),
        )
    )
