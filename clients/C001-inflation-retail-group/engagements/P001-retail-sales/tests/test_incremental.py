"""
Tests for P001 incremental current-state processing.
"""

from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

from p001_retail_sales.incremental import merge_sales_current_state
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
    validation.
    """
    products_df, stores_df = _get_validated_reference_data(spark)

    raw_sales_df = None

    for filename in filenames:
        current_df = read_raw_sales(
            spark,
            str(TEST_INPUT / 'sales' / filename),
        )

        if raw_sales_df is None:
            raw_sales_df = current_df
        else:
            raw_sales_df = raw_sales_df.unionByName(
                current_df
            )

    validated_df = validate_sales(
        standardize_sales(raw_sales_df),
        products_df,
        stores_df,
    )

    accepted_df, _ = split_validated_records(
        validated_df
    )

    return accepted_df


def _get_accepted_sales_from_rows(spark, rows):
    """
    Build accepted sales records directly from raw test rows.
    """
    products_df, stores_df = _get_validated_reference_data(spark)

    raw_df = spark.createDataFrame(
        rows,
        schema=RAW_SALES_SCHEMA,
    )

    validated_df = validate_sales(
        standardize_sales(raw_df),
        products_df,
        stores_df,
    )

    accepted_df, quarantined_df = split_validated_records(
        validated_df
    )

    # The synthetic records used by incremental tests are expected
    # to be individually valid.
    assert quarantined_df.count() == 0

    return accepted_df


def _filter_business_key(df, order_id, line_id):
    """
    Select one logical order line.
    """
    return df.filter(
        (F.col('order_id') == order_id)
        & (F.col('line_id') == line_id)
    )


def _resolve_current_state(df):
    """
    Produce a previously trusted current state for an incremental test.
    """
    resolved_df, ambiguous_df = resolve_sales_versions(df)

    assert ambiguous_df.count() == 0

    return resolved_df


# =============================================================================
# FIRST RUN
# =============================================================================


def test_incremental_first_run_resolves_incoming_records(spark):
    incoming_df = _get_accepted_sales(
        spark,
        'sales_2026-10-01.csv',
    )

    incoming_df = _filter_business_key(
        incoming_df,
        'O1001',
        1,
    )

    next_state_df, ambiguous_df = merge_sales_current_state(
        None,
        incoming_df,
    )

    rows = _filter_business_key(
        next_state_df,
        'O1001',
        1,
    ).collect()

    assert len(rows) == 1
    assert ambiguous_df.count() == 0
    assert rows[0]['unit_price'] == Decimal('5.00')


# =============================================================================
# NEWER CORRECTION
# =============================================================================


def test_incremental_newer_version_replaces_current_state(spark):
    day1_df = _get_accepted_sales(
        spark,
        'sales_2026-10-01.csv',
    )
    day2_df = _get_accepted_sales(
        spark,
        'sales_2026-10-02.csv',
    )

    current_df = _resolve_current_state(
        _filter_business_key(
            day1_df,
            'O1001',
            1,
        )
    )

    incoming_df = _filter_business_key(
        day2_df,
        'O1001',
        1,
    )

    next_state_df, ambiguous_df = merge_sales_current_state(
        current_df,
        incoming_df,
    )

    rows = _filter_business_key(
        next_state_df,
        'O1001',
        1,
    ).collect()

    assert len(rows) == 1
    assert ambiguous_df.count() == 0

    # Day 2 contains the newer correction.
    assert rows[0]['unit_price'] == Decimal('5.50')
    assert rows[0]['discount_amount'] == Decimal('0.25')


# =============================================================================
# STALE INCOMING VERSION
# =============================================================================


def test_incremental_stale_version_does_not_replace_current_state(spark):
    day1_df = _get_accepted_sales(
        spark,
        'sales_2026-10-01.csv',
    )
    day2_df = _get_accepted_sales(
        spark,
        'sales_2026-10-02.csv',
    )

    # Day 1 contains the newer authoritative O1002/1 record.
    current_df = _resolve_current_state(
        _filter_business_key(
            day1_df,
            'O1002',
            1,
        )
    )

    # Day 2 redelivers an older version of the same business key.
    incoming_df = _filter_business_key(
        day2_df,
        'O1002',
        1,
    )

    next_state_df, ambiguous_df = merge_sales_current_state(
        current_df,
        incoming_df,
    )

    rows = _filter_business_key(
        next_state_df,
        'O1002',
        1,
    ).collect()

    assert len(rows) == 1
    assert ambiguous_df.count() == 0

    # The already-current newer version must remain authoritative.
    assert rows[0]['unit_price'] == Decimal('79.99')
    assert rows[0]['discount_amount'] == Decimal('10.00')


# =============================================================================
# EXACT REPLAY
# =============================================================================


def test_incremental_exact_replay_is_idempotent(spark):
    day2_df = _get_accepted_sales(
        spark,
        'sales_2026-10-02.csv',
    )
    day3_df = _get_accepted_sales(
        spark,
        'sales_2026-10-03.csv',
    )

    current_df = _resolve_current_state(
        _filter_business_key(
            day2_df,
            'O1008',
            1,
        )
    )

    # Day 3 replays O1008/1 exactly.
    incoming_df = _filter_business_key(
        day3_df,
        'O1008',
        1,
    )

    next_state_df, ambiguous_df = merge_sales_current_state(
        current_df,
        incoming_df,
    )

    rows = _filter_business_key(
        next_state_df,
        'O1008',
        1,
    ).collect()

    assert len(rows) == 1
    assert ambiguous_df.count() == 0


# =============================================================================
# NEW BUSINESS KEY
# =============================================================================


def test_incremental_adds_new_business_key_without_losing_existing_state(
    spark,
):
    day1_df = _get_accepted_sales(
        spark,
        'sales_2026-10-01.csv',
    )
    day2_df = _get_accepted_sales(
        spark,
        'sales_2026-10-02.csv',
    )

    current_df = _resolve_current_state(
        _filter_business_key(
            day1_df,
            'O1001',
            1,
        )
    )

    incoming_df = _filter_business_key(
        day2_df,
        'O1008',
        1,
    )

    next_state_df, ambiguous_df = merge_sales_current_state(
        current_df,
        incoming_df,
    )

    existing_count = _filter_business_key(
        next_state_df,
        'O1001',
        1,
    ).count()

    new_count = _filter_business_key(
        next_state_df,
        'O1008',
        1,
    ).count()

    assert existing_count == 1
    assert new_count == 1
    assert next_state_df.count() == 2
    assert ambiguous_df.count() == 0


# =============================================================================
# CROSS-BATCH AMBIGUITY
# =============================================================================


def test_incremental_same_timestamp_conflict_becomes_ambiguous(spark):
    current_accepted_df = _get_accepted_sales_from_rows(
        spark,
        [
            (
                'O2000',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '12.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T10:00:00',
            ),
        ],
    )

    current_df = _resolve_current_state(
        current_accepted_df
    )

    incoming_df = _get_accepted_sales_from_rows(
        spark,
        [
            (
                'O2000',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '15.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T10:00:00',
            ),
        ],
    )

    next_state_df, ambiguous_df = merge_sales_current_state(
        current_df,
        incoming_df,
    )

    resolved_count = _filter_business_key(
        next_state_df,
        'O2000',
        1,
    ).count()

    ambiguous_rows = _filter_business_key(
        ambiguous_df,
        'O2000',
        1,
    ).collect()

    # The previously trusted $12 row must be revoked from current
    # state when an equally recent conflicting source version arrives.
    assert resolved_count == 0
    assert len(ambiguous_rows) == 2

    prices = {
        row['unit_price']
        for row in ambiguous_rows
    }

    assert prices == {
        Decimal('12.00'),
        Decimal('15.00'),
    }

    for row in ambiguous_rows:
        assert (
            'AMBIGUOUS_LATEST_VERSION'
            in row['rejection_reasons']
        )


def test_incremental_ambiguity_does_not_remove_unrelated_current_records(
    spark,
):
    current_accepted_df = _get_accepted_sales_from_rows(
        spark,
        [
            (
                'O2000',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '12.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T10:00:00',
            ),
            (
                'O2001',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '2',
                '8.00',
                '5.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T09:00:00',
            ),
        ],
    )

    current_df = _resolve_current_state(
        current_accepted_df
    )

    incoming_df = _get_accepted_sales_from_rows(
        spark,
        [
            (
                'O2000',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '15.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T10:00:00',
            ),
        ],
    )

    next_state_df, ambiguous_df = merge_sales_current_state(
        current_df,
        incoming_df,
    )

    # O2000 becomes ambiguous.
    assert (
        _filter_business_key(
            next_state_df,
            'O2000',
            1,
        ).count()
        == 0
    )

    assert (
        _filter_business_key(
            ambiguous_df,
            'O2000',
            1,
        ).count()
        == 2
    )

    # O2001 is unrelated and must remain trusted.
    unaffected_rows = _filter_business_key(
        next_state_df,
        'O2001',
        1,
    ).collect()

    assert len(unaffected_rows) == 1
    assert unaffected_rows[0]['quantity'] == 2
    assert unaffected_rows[0]['unit_price'] == Decimal('8.00')


def test_incremental_preserves_unresolved_ambiguity_across_runs(spark):
    current_df = _get_accepted_sales_from_rows(
        spark,
        [
            (
                'O3000',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '12.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T10:00:00',
            ),
        ],
    )

    conflicting_df = _get_accepted_sales_from_rows(
        spark,
        [
            (
                'O3000',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '15.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T10:00:00',
            ),
        ],
    )

    # Run 1 discovers the conflict.
    next_state_df, ambiguous_df = merge_sales_current_state(
        current_df,
        conflicting_df,
    )

    assert (
        _filter_business_key(
            next_state_df,
            'O3000',
            1,
        ).count()
        == 0
    )

    assert (
        _filter_business_key(
            ambiguous_df,
            'O3000',
            1,
        ).count()
        == 2
    )

    # Run 2 replays only one side of the conflict.
    replay_df = conflicting_df

    next_state_df, next_ambiguous_df = (
        merge_sales_current_state(
            next_state_df,
            replay_df,
            ambiguous_state_df=ambiguous_df,
        )
    )

    # The conflict must NOT be forgotten.
    assert (
        _filter_business_key(
            next_state_df,
            'O3000',
            1,
        ).count()
        == 0
    )

    rows = _filter_business_key(
        next_ambiguous_df,
        'O3000',
        1,
    ).collect()

    assert len(rows) == 2

    for row in rows:
        assert row['rejection_reasons'].count(
            'AMBIGUOUS_LATEST_VERSION'
        ) == 1


def test_incremental_newer_version_resolves_prior_ambiguity(spark):
    ambiguous_state_df = _get_accepted_sales_from_rows(
        spark,
        [
            (
                'O3001',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '12.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T10:00:00',
            ),
            (
                'O3001',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '15.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T10:00:00',
            ),
        ],
    )

    _, ambiguous_state_df = resolve_sales_versions(
        ambiguous_state_df
    )

    corrective_df = _get_accepted_sales_from_rows(
        spark,
        [
            (
                'O3001',
                '1',
                '2026-10-03',
                'S001',
                'P001',
                '1',
                '15.00',
                '7.00',
                '0.00',
                'COMPLETED',
                '2026-10-03T11:00:00',
            ),
        ],
    )

    next_state_df, next_ambiguous_df = (
        merge_sales_current_state(
            None,
            corrective_df,
            ambiguous_state_df=ambiguous_state_df,
        )
    )

    rows = _filter_business_key(
        next_state_df,
        'O3001',
        1,
    ).collect()

    assert len(rows) == 1
    assert rows[0]['unit_price'] == Decimal('15.00')

    assert (
        _filter_business_key(
            next_ambiguous_df,
            'O3001',
            1,
        ).count()
        == 0
    )
