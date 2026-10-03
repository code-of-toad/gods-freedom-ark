"""
End-to-end integration tests for the P001 sales pipeline.
"""
import pytest
from decimal import Decimal
from pathlib import Path
from pyspark.sql import functions as F

from p001_retail_sales.pipeline import (
    ReferenceDataError,
    run_sales_pipeline,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_INPUT = PROJECT_ROOT / 'data' / 'test' / 'input'

PRODUCTS_PATH = str(TEST_INPUT / 'products.csv')

STORES_PATH = str(TEST_INPUT / 'stores.csv')

SALES_PATH = TEST_INPUT / 'sales'


def _business_key(
    df,
    order_id,
    line_id,
):
    return df.filter(
        (F.col('order_id') == order_id)
        & (F.col('line_id') == line_id)
    )


def _run_fixture_batch(
    spark,
    filename,
    current_state_df=None,
    ambiguous_state_df=None,
):
    return run_sales_pipeline(
        spark=spark,
        sales_path=str(
            SALES_PATH / filename
        ),
        products_path=PRODUCTS_PATH,
        stores_path=STORES_PATH,
        current_state_df=current_state_df,
        ambiguous_state_df=ambiguous_state_df,
    )


# =============================================================================
# FIRST END-TO-END RUN
# =============================================================================


def test_pipeline_first_run_produces_reconciled_candidate(spark):
    result = _run_fixture_batch(
        spark,
        'sales_2026-10-01.csv',
    )

    # Day 1 resolves to:
    #
    # O1001/1
    # O1001/2
    # O1002/1
    #
    # The exact O1002 duplicate must not be double-counted.
    assert result.resolved_state_df.count() == 3
    assert result.candidate_df.count() == 3

    assert result.ambiguous_state_df.count() == 0

    # Five Day 1 records fail validation / referential integrity.
    assert result.validation_quarantine_df.count() == 5

    # The canonical fixture reference data is valid.
    assert result.products_quarantine_df.count() == 0
    assert result.stores_quarantine_df.count() == 0

    row = (
        _business_key(
            result.candidate_df,
            'O1001',
            1,
        )
        .first()
    )

    assert row['gross_sales'] == Decimal('10.00')
    assert row['net_sales'] == Decimal('10.00')
    assert row['gross_margin'] == Decimal('4.00')


# =============================================================================
# MULTI-BATCH CORRECTION HANDLING
# =============================================================================


def test_pipeline_second_run_applies_newer_correction(spark):
    run_1 = _run_fixture_batch(
        spark,
        'sales_2026-10-01.csv',
    )

    run_2 = _run_fixture_batch(
        spark,
        'sales_2026-10-02.csv',
        current_state_df=run_1.resolved_state_df,
        ambiguous_state_df=run_1.ambiguous_state_df,
    )

    corrected_row = (
        _business_key(
            run_2.candidate_df,
            'O1001',
            1,
        )
        .first()
    )

    assert corrected_row['unit_price'] == Decimal('5.50')
    assert corrected_row['discount_amount'] == Decimal('0.25')

    # O1002/1 receives a stale version on Day 2.
    # Its newer Day 1 state must survive.
    stale_target = (
        _business_key(
            run_2.candidate_df,
            'O1002',
            1,
        )
        .first()
    )

    assert stale_target['unit_price'] == Decimal('79.99')
    assert stale_target['discount_amount'] == Decimal('10.00')


# =============================================================================
# AMBIGUOUS VERSION HANDLING
# =============================================================================


def test_pipeline_third_run_excludes_ambiguous_latest_version(spark):
    run_1 = _run_fixture_batch(
        spark,
        'sales_2026-10-01.csv',
    )

    run_2 = _run_fixture_batch(
        spark,
        'sales_2026-10-02.csv',
        current_state_df=run_1.resolved_state_df,
        ambiguous_state_df=run_1.ambiguous_state_df,
    )

    run_3 = _run_fixture_batch(
        spark,
        'sales_2026-10-03.csv',
        current_state_df=run_2.resolved_state_df,
        ambiguous_state_df=run_2.ambiguous_state_df,
    )

    # O1014/1 has conflicting latest versions.
    assert (
        _business_key(
            run_3.candidate_df,
            'O1014',
            1,
        ).count()
        == 0
    )

    ambiguous_rows = (
        _business_key(
            run_3.ambiguous_state_df,
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

    for row in ambiguous_rows:
        assert (
            'AMBIGUOUS_LATEST_VERSION'
            in row['rejection_reasons']
        )


def test_pipeline_replay_preserves_existing_ambiguity(spark):
    run_1 = _run_fixture_batch(
        spark,
        'sales_2026-10-01.csv',
    )

    run_2 = _run_fixture_batch(
        spark,
        'sales_2026-10-02.csv',
        current_state_df=run_1.resolved_state_df,
        ambiguous_state_df=run_1.ambiguous_state_df,
    )

    run_3 = _run_fixture_batch(
        spark,
        'sales_2026-10-03.csv',
        current_state_df=run_2.resolved_state_df,
        ambiguous_state_df=run_2.ambiguous_state_df,
    )

    # Replay Day 3.
    replay = _run_fixture_batch(
        spark,
        'sales_2026-10-03.csv',
        current_state_df=run_3.resolved_state_df,
        ambiguous_state_df=run_3.ambiguous_state_df,
    )

    assert (
        _business_key(
            replay.candidate_df,
            'O1014',
            1,
        ).count()
        == 0
    )

    ambiguous_rows = (
        _business_key(
            replay.ambiguous_state_df,
            'O1014',
            1,
        )
        .collect()
    )

    assert len(ambiguous_rows) == 2

    for row in ambiguous_rows:
        assert row['rejection_reasons'].count(
            'AMBIGUOUS_LATEST_VERSION'
        ) == 1


# =============================================================================
# REFERENCE-DATA FAILURE POLICY
# =============================================================================


def test_pipeline_fails_on_duplicate_product_ids(
    spark,
    tmp_path,
):
    products_path = tmp_path / 'products.csv'
    stores_path = tmp_path / 'stores.csv'
    sales_path = tmp_path / 'sales.csv'

    products_path.write_text(
        (
            'product_id,product_name,category,active\n'
            'P001,Coffee,Grocery,true\n'
            'P001,Coffee Duplicate,Grocery,true\n'
        ),
        encoding='utf-8',
    )

    stores_path.write_text(
        (
            'store_id,store_name,city,province,active\n'
            'S001,Test Store,Toronto,ON,true\n'
        ),
        encoding='utf-8',
    )

    sales_path.write_text(
        (
            'order_id,line_id,sale_date,store_id,product_id,'
            'quantity,unit_price,unit_cost,discount_amount,'
            'order_status,updated_at\n'
            'O9001,1,2026-10-03,S001,P001,1,10.00,6.00,'
            '0.00,COMPLETED,2026-10-03T10:00:00\n'
        ),
        encoding='utf-8',
    )

    with pytest.raises(
        ReferenceDataError,
        match='DUPLICATE_PRODUCT_ID',
    ):
        run_sales_pipeline(
            spark=spark,
            sales_path=str(sales_path),
            products_path=str(products_path),
            stores_path=str(stores_path),
        )


def test_pipeline_quarantines_invalid_reference_and_dependent_sale(
    spark,
    tmp_path,
):
    products_path = tmp_path / 'products.csv'
    stores_path = tmp_path / 'stores.csv'
    sales_path = tmp_path / 'sales.csv'

    # P999 physically exists but is not a trustworthy canonical
    # reference because active cannot be standardized.
    products_path.write_text(
        (
            'product_id,product_name,category,active\n'
            'P999,Invalid Product,Grocery,banana\n'
        ),
        encoding='utf-8',
    )

    stores_path.write_text(
        (
            'store_id,store_name,city,province,active\n'
            'S001,Test Store,Toronto,ON,true\n'
        ),
        encoding='utf-8',
    )

    sales_path.write_text(
        (
            'order_id,line_id,sale_date,store_id,product_id,'
            'quantity,unit_price,unit_cost,discount_amount,'
            'order_status,updated_at\n'
            'O9002,1,2026-10-03,S001,P999,1,10.00,6.00,'
            '0.00,COMPLETED,2026-10-03T10:00:00\n'
        ),
        encoding='utf-8',
    )

    result = run_sales_pipeline(
        spark=spark,
        sales_path=str(sales_path),
        products_path=str(products_path),
        stores_path=str(stores_path),
    )

    assert result.products_quarantine_df.count() == 1
    assert result.candidate_df.count() == 0
    assert result.validation_quarantine_df.count() == 1

    product_row = result.products_quarantine_df.first()
    sales_row = result.validation_quarantine_df.first()

    assert (
        'INVALID_ACTIVE'
        in product_row['rejection_reasons']
    )

    assert (
        'UNKNOWN_PRODUCT_ID'
        in sales_row['rejection_reasons']
    )
