"""
Duplicate and version resolution for P001 sales.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SALES_BUSINESS_KEY = [
    'order_id',
    'line_id',
]

SALES_CANONICAL_COLUMNS = [
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
]


def resolve_sales_versions(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Resolve duplicate and versioned sales records.

    Returns:
        resolved_df:
            One authoritative current row per business key.

        ambiguous_df:
            Conflicting rows sharing the same business key and updated_at.
    """
    # Collapse canonically identical redeliveries.
    deduplicated_df = df.dropDuplicates(SALES_CANONICAL_COLUMNS)

    # Determine the newest source version timestamp for each
    # logical sales record.
    latest_window = Window.partitionBy(*SALES_BUSINESS_KEY)
    latest_df = (
        deduplicated_df
        .withColumn(
            '_latest_updated_at',
            F.max('updated_at').over(latest_window)
        )
        .filter(F.col('updated_at') == F.col('_latest_updated_at'))
        .drop('_latest_updated_at')
    )

    # Once exact duplicates have been removed, more than one row
    # remaining for the latest timestamp means that the newest
    # source version is conflicting and has no deterministic winner.
    latest_version_window = Window.partitionBy(
        *SALES_BUSINESS_KEY,
        'updated_at',
    )
    classified_df = latest_df.withColumn(
        '_version_count',
        F.count('*').over(latest_version_window)
    )

    resolved_df = (
        classified_df
        .filter(F.col('_version_count') == 1)
        .drop('_version_count')
    )
    ambiguous_df = (
        classified_df
        .filter(F.col('_version_count') > 1)
        .withColumn(
            'rejection_reasons',
            F.concat(
                F.col('rejection_reasons'),
                F.array(F.lit('AMBIGUOUS_LATEST_VERSION'))
            )
        )
        .drop('_version_count')
    )

    return resolved_df, ambiguous_df
