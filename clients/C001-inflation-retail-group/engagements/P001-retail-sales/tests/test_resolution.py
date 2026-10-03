"""
Tests for P001 sales duplicate and version resolution.
"""
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

from p001_retail_sales.ingestion import (
    read_raw_products,
    read_raw_sales,
    read_raw_stores,
)
from p001_retail_sales.resolution import resolve_sales_versions
from p001_retail_sales.schemas import RAW_SALES_SCHEMA
from p001_retail_sales.standardization import (
    standardize_products,
    standardize_sales,
    standardize_stores,
)
from p001_retail_sales.validation import (
    add_products_rejection_reasons,
    add_stores_rejection_reasons,
    split_validated_records,
    validate_sales,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_INPUT = PROJECT_ROOT / 'data' / 'test' / 'input'


# =============================================================================
# HELPERS
# =============================================================================


def _get_validated_reference_data(spark):
    """
    Load and validate the shared product and store reference data.
    """
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


def _get_accepted_sales(spark, *filenames):
    """
    Load one or more sales deliveries and return only rows that pass
    row-level and referential-integrity validation.
    """
    products_df, stores_df = _get_validated_reference_data(spark)

    raw_sales_df = None

    # Combine the requested source deliveries before applying the
    # standardization/validation workflow.
    for filename in filenames:
        current_df = read_raw_sales(
            spark,
            str(TEST_INPUT / 'sales' / filename),
        )

        if raw_sales_df is None:
            raw_sales_df = current_df
        else:
            raw_sales_df = raw_sales_df.unionByName(current_df)

    standardized_df = standardize_sales(raw_sales_df)

    validated_df = validate_sales(
        standardized_df,
        products_df,
        stores_df,
    )

    accepted_df, _ = split_validated_records(validated_df)

    return accepted_df


# =============================================================================
# EXACT DUPLICATE RESOLUTION
# =============================================================================


def test_resolution_collapses_exact_duplicate(spark):
    accepted_df = _get_accepted_sales(
        spark,
        'sales_2026-10-01.csv',
    )

    resolved_df, ambiguous_df = resolve_sales_versions(
        accepted_df
    )

    # O1002/1 appears twice identically in the Day 1 delivery.
    # Resolution should retain exactly one canonical version.
    resolved_count = (
        resolved_df
        .filter(
            (F.col('order_id') == 'O1002')
            & (F.col('line_id') == 1)
        )
        .count()
    )

    ambiguous_count = (
        ambiguous_df
        .filter(
            (F.col('order_id') == 'O1002')
            & (F.col('line_id') == 1)
        )
        .count()
    )

    assert resolved_count == 1
    assert ambiguous_count == 0


# =============================================================================
# VERSION PRECEDENCE
# =============================================================================


def test_resolution_selects_newer_correction(spark):
    accepted_df = _get_accepted_sales(
        spark,
        'sales_2026-10-01.csv',
        'sales_2026-10-02.csv',
    )

    resolved_df, _ = resolve_sales_versions(
        accepted_df
    )

    # Day 2 contains a newer correction for O1001/1.
    row = (
        resolved_df
        .filter(
            (F.col('order_id') == 'O1001')
            & (F.col('line_id') == 1)
        )
        .first()
    )

    assert row['unit_price'] == Decimal('5.50')
    assert row['discount_amount'] == Decimal('0.25')


def test_resolution_does_not_replace_newer_state_with_stale_version(spark):
    accepted_df = _get_accepted_sales(
        spark,
        'sales_2026-10-01.csv',
        'sales_2026-10-02.csv',
    )

    resolved_df, _ = resolve_sales_versions(
        accepted_df
    )

    # Day 2 delivers an older version of O1002/1.
    # The already-newer Day 1 version must remain authoritative.
    row = (
        resolved_df
        .filter(
            (F.col('order_id') == 'O1002')
            & (F.col('line_id') == 1)
        )
        .first()
    )

    assert row['unit_price'] == Decimal('79.99')
    assert row['discount_amount'] == Decimal('10.00')


# =============================================================================
# REPLAY / IDEMPOTENCY
# =============================================================================


def test_resolution_collapses_exact_replay_across_deliveries(spark):
    accepted_df = _get_accepted_sales(
        spark,
        'sales_2026-10-02.csv',
        'sales_2026-10-03.csv',
    )

    resolved_df, ambiguous_df = resolve_sales_versions(
        accepted_df
    )

    # O1008/1 is delivered on Day 2 and replayed exactly on Day 3.
    resolved_count = (
        resolved_df
        .filter(
            (F.col('order_id') == 'O1008')
            & (F.col('line_id') == 1)
        )
        .count()
    )

    ambiguous_count = (
        ambiguous_df
        .filter(
            (F.col('order_id') == 'O1008')
            & (F.col('line_id') == 1)
        )
        .count()
    )

    assert resolved_count == 1
    assert ambiguous_count == 0


# =============================================================================
# AMBIGUOUS LATEST VERSION
# =============================================================================


def test_resolution_quarantines_conflicting_latest_versions(spark):
    accepted_df = _get_accepted_sales(
        spark,
        'sales_2026-10-03.csv',
    )

    resolved_df, ambiguous_df = resolve_sales_versions(
        accepted_df
    )

    # O1014/1 has two different canonical records with the same
    # latest updated_at. There is no deterministic winner.
    resolved_count = (
        resolved_df
        .filter(
            (F.col('order_id') == 'O1014')
            & (F.col('line_id') == 1)
        )
        .count()
    )

    ambiguous_rows = (
        ambiguous_df
        .filter(
            (F.col('order_id') == 'O1014')
            & (F.col('line_id') == 1)
        )
        .collect()
    )

    assert resolved_count == 0
    assert len(ambiguous_rows) == 2

    prices = {
        row['unit_price']
        for row in ambiguous_rows
    }

    assert prices == {
        Decimal('20.00'),
        Decimal('22.00'),
    }

    for row in ambiguous_rows:
        assert (
            'AMBIGUOUS_LATEST_VERSION'
            in row['rejection_reasons']
        )


def test_resolution_does_not_fall_back_when_latest_version_is_ambiguous(
    spark,
):
    """
    Guard against resolving an older valid version when the newest
    timestamp contains conflicting versions.
    """
    products_df, stores_df = _get_validated_reference_data(spark)

    # Existing Day 3 data contains two conflicting latest versions
    # of O1014/1 at 10:00.
    accepted_day3_df = _get_accepted_sales(
        spark,
        'sales_2026-10-03.csv',
    )

    # Introduce an older, individually valid version of the same
    # business key.
    older_raw_df = spark.createDataFrame(
        [
            (
                'O1014',
                '1',
                '2026-10-03',
                'S001',
                'P009',
                '1',
                '18.00',
                '12.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T09:00:00',
            ),
        ],
        schema=RAW_SALES_SCHEMA,
    )

    older_validated_df = validate_sales(
        standardize_sales(older_raw_df),
        products_df,
        stores_df,
    )

    older_accepted_df, older_quarantined_df = (
        split_validated_records(older_validated_df)
    )

    # Sanity check: the older version itself is perfectly valid.
    assert older_accepted_df.count() == 1
    assert older_quarantined_df.count() == 0

    combined_df = accepted_day3_df.unionByName(
        older_accepted_df
    )

    resolved_df, ambiguous_df = resolve_sales_versions(
        combined_df
    )

    resolved_count = (
        resolved_df
        .filter(
            (F.col('order_id') == 'O1014')
            & (F.col('line_id') == 1)
        )
        .count()
    )

    ambiguous_rows = (
        ambiguous_df
        .filter(
            (F.col('order_id') == 'O1014')
            & (F.col('line_id') == 1)
        )
        .collect()
    )

    # The older $18 version must NOT become authoritative merely
    # because the newer 10:00 versions conflict.
    assert resolved_count == 0
    assert len(ambiguous_rows) == 2

    ambiguous_prices = {
        row['unit_price']
        for row in ambiguous_rows
    }

    assert ambiguous_prices == {
        Decimal('20.00'),
        Decimal('22.00'),
    }
