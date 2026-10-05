"""
Persistent end-to-end job tests for P001 sales.
"""

from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

from p001_retail_sales.job import run_sales_batch_job
from p001_retail_sales.publication import (
    get_current_run_id,
    load_current_analytical_table,
    load_current_curated_sales,
    load_current_sales_state,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_INPUT = PROJECT_ROOT / 'data' / 'test' / 'input'

PRODUCTS_PATH = TEST_INPUT / 'products.csv'
STORES_PATH = TEST_INPUT / 'stores.csv'
SALES_ROOT = TEST_INPUT / 'sales'


def _business_key(
    df,
    order_id,
    line_id,
):
    """
    Filter a DataFrame to one sales business key.
    """
    return df.filter(
        (F.col('order_id') == order_id)
        & (F.col('line_id') == line_id)
    )


def _run_batch(
    spark,
    output_root,
    quarantine_root,
    filename,
    run_id,
):
    """
    Execute one fixture delivery through the complete persistent job.
    """
    return run_sales_batch_job(
        spark=spark,
        sales_path=SALES_ROOT / filename,
        products_path=PRODUCTS_PATH,
        stores_path=STORES_PATH,
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id=run_id,
    )


# =============================================================================
# FIRST PERSISTENT RUN
# =============================================================================


def test_job_first_run_publishes_current_state(
    spark,
    tmp_path,
):
    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-01.csv',
        'run-001',
    )

    assert (
        get_current_run_id(output_root)
        == 'run-001'
    )

    current_state_df, ambiguous_state_df = (
        load_current_sales_state(
            spark,
            output_root,
            quarantine_root,
        )
    )

    assert current_state_df.count() == 3
    assert ambiguous_state_df.count() == 0

    curated_df = load_current_curated_sales(
        spark,
        output_root,
    )

    assert curated_df.count() == 3

    fact_sales_df = load_current_analytical_table(
        spark,
        output_root,
        'fact_sales',
    )

    dim_product_df = load_current_analytical_table(
        spark,
        output_root,
        'dim_product',
    )

    dim_store_df = load_current_analytical_table(
        spark,
        output_root,
        'dim_store',
    )

    dim_date_df = load_current_analytical_table(
        spark,
        output_root,
        'dim_date',
    )

    assert fact_sales_df.count() == 3
    assert dim_product_df.count() == 10
    assert dim_store_df.count() == 5
    assert dim_date_df.count() == 1


# =============================================================================
# STATE AUTOMATICALLY FLOWS BETWEEN RUNS
# =============================================================================


def test_job_second_run_automatically_uses_previous_state(
    spark,
    tmp_path,
):
    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-01.csv',
        'run-001',
    )

    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-02.csv',
        'run-002',
    )

    assert (
        get_current_run_id(output_root)
        == 'run-002'
    )

    curated_df = load_current_curated_sales(
        spark,
        output_root,
    )

    corrected_row = (
        _business_key(
            curated_df,
            'O1001',
            1,
        )
        .first()
    )

    assert corrected_row['unit_price'] == Decimal('5.50')
    assert (
        corrected_row['discount_amount']
        == Decimal('0.25')
    )

    # Day 2 contains a stale O1002/1 version.
    # The newer state persisted from Day 1 must survive.
    stale_target = (
        _business_key(
            curated_df,
            'O1002',
            1,
        )
        .first()
    )

    assert stale_target['unit_price'] == Decimal('79.99')
    assert (
        stale_target['discount_amount']
        == Decimal('10.00')
    )


# =============================================================================
# THREE-DAY INCREMENTAL LIFECYCLE
# =============================================================================


def test_job_three_day_sequence_persists_ambiguity(
    spark,
    tmp_path,
):
    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-01.csv',
        'run-001',
    )

    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-02.csv',
        'run-002',
    )

    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-03.csv',
        'run-003',
    )

    assert (
        get_current_run_id(output_root)
        == 'run-003'
    )

    current_state_df, ambiguous_state_df = (
        load_current_sales_state(
            spark,
            output_root,
            quarantine_root,
        )
    )

    # O1014/1 has two conflicting versions at the latest timestamp.
    assert (
        _business_key(
            current_state_df,
            'O1014',
            1,
        ).count()
        == 0
    )

    ambiguous_rows = (
        _business_key(
            ambiguous_state_df,
            'O1014',
            1,
        )
        .collect()
    )

    assert len(ambiguous_rows) == 2

    prices = {
        row['unit_price']
        for row in ambiguous_rows
    }

    assert prices == {
        Decimal('20.00'),
        Decimal('22.00'),
    }


# =============================================================================
# PERSISTENT REPLAY / IDEMPOTENCY
# =============================================================================


def test_job_replaying_delivery_preserves_logical_state(
    spark,
    tmp_path,
):
    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-01.csv',
        'run-001',
    )

    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-02.csv',
        'run-002',
    )

    before_replay = load_current_curated_sales(
        spark,
        output_root,
    )

    before_rows = (
        before_replay
        .select(
            'order_id',
            'line_id',
            'updated_at',
            'gross_sales',
            'net_sales',
            'gross_margin',
        )
        .collect()
    )

    # Process the same delivery again as a new physical pipeline run.
    _run_batch(
        spark,
        output_root,
        quarantine_root,
        'sales_2026-10-02.csv',
        'run-003',
    )

    after_replay = load_current_curated_sales(
        spark,
        output_root,
    )

    after_rows = (
        after_replay
        .select(
            'order_id',
            'line_id',
            'updated_at',
            'gross_sales',
            'net_sales',
            'gross_margin',
        )
        .collect()
    )

    assert {
        tuple(row)
        for row in before_rows
    } == {
        tuple(row)
        for row in after_rows
    }

    assert (
        get_current_run_id(output_root)
        == 'run-003'
    )
