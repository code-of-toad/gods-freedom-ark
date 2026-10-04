"""
Tests for P001 local Parquet publication.
"""

from decimal import Decimal

import pytest
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from p001_retail_sales.pipeline import SalesPipelineResult
from p001_retail_sales.publication import (
    get_current_run_id,
    load_current_curated_sales,
    load_current_sales_state,
    publish_sales_run,
)


STATE_SCHEMA = StructType([
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
        'rejection_reasons',
        ArrayType(
            StringType(),
            containsNull=False,
        ),
        nullable=False,
    ),
])


def _state_df(
    spark,
    price='10.00',
):
    return spark.createDataFrame(
        [
            (
                'O5000',
                1,
                1,
                Decimal(price),
                Decimal('6.00'),
                Decimal('0.00'),
                'COMPLETED',
                [],
            ),
        ],
        schema=STATE_SCHEMA,
    )


def _result(
    spark,
    price='10.00',
):
    resolved_df = _state_df(
        spark,
        price,
    )

    candidate_df = (
        resolved_df
        .withColumn(
            'gross_sales',
            (
                resolved_df['quantity']
                * resolved_df['unit_price']
            ).cast('decimal(24,2)'),
        )
        .withColumn(
            'net_sales',
            (
                resolved_df['quantity']
                * resolved_df['unit_price']
                - resolved_df['discount_amount']
            ).cast('decimal(24,2)'),
        )
        .withColumn(
            'gross_margin',
            (
                resolved_df['quantity']
                * resolved_df['unit_price']
                - resolved_df['discount_amount']
                - (
                    resolved_df['quantity']
                    * resolved_df['unit_cost']
                )
            ).cast('decimal(24,2)'),
        )
    )

    empty_df = resolved_df.limit(0)

    return SalesPipelineResult(
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        validation_quarantine_df=empty_df,
        ambiguous_state_df=empty_df,
        products_quarantine_df=empty_df,
        stores_quarantine_df=empty_df,
    )


def test_publication_first_run_becomes_current(
    spark,
    tmp_path,
):
    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    publish_sales_run(
        result=_result(spark),
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id='run-001',
    )

    assert (
        get_current_run_id(output_root)
        == 'run-001'
    )

    assert (
        output_root
        / 'runs'
        / 'run-001'
        / 'resolved_state'
    ).exists()

    assert (
        output_root
        / 'runs'
        / 'run-001'
        / 'curated_sales'
    ).exists()


def test_publication_state_round_trip(
    spark,
    tmp_path,
):
    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    publish_sales_run(
        result=_result(
            spark,
            price='12.00',
        ),
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id='run-001',
    )

    current_df, ambiguous_df = (
        load_current_sales_state(
            spark,
            output_root,
            quarantine_root,
        )
    )

    row = current_df.first()

    assert row['order_id'] == 'O5000'
    assert row['unit_price'] == Decimal('12.00')
    assert ambiguous_df.count() == 0


def test_publication_loads_curated_output(
    spark,
    tmp_path,
):
    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    publish_sales_run(
        result=_result(
            spark,
            price='10.00',
        ),
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id='run-001',
    )

    curated_df = load_current_curated_sales(
        spark,
        output_root,
    )

    row = curated_df.first()

    assert row['gross_sales'] == Decimal('10.00')
    assert row['gross_margin'] == Decimal('4.00')


def test_publication_new_run_preserves_previous_snapshot(
    spark,
    tmp_path,
):
    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    publish_sales_run(
        result=_result(
            spark,
            price='10.00',
        ),
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id='run-001',
    )

    publish_sales_run(
        result=_result(
            spark,
            price='15.00',
        ),
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id='run-002',
    )

    assert (
        get_current_run_id(output_root)
        == 'run-002'
    )

    # Previous known-good snapshot still physically exists.
    assert (
        output_root
        / 'runs'
        / 'run-001'
        / 'resolved_state'
    ).exists()

    current_df, _ = load_current_sales_state(
        spark,
        output_root,
        quarantine_root,
    )

    assert (
        current_df.first()['unit_price']
        == Decimal('15.00')
    )


def test_failed_publication_preserves_previous_current_run(
    spark,
    tmp_path,
    monkeypatch,
):
    import p001_retail_sales.publication as publication

    output_root = tmp_path / 'output'
    quarantine_root = tmp_path / 'quarantine'

    publish_sales_run(
        result=_result(
            spark,
            price='10.00',
        ),
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id='run-001',
    )

    original_write = publication._write_parquet

    call_count = 0

    def failing_write(df, path):
        nonlocal call_count

        call_count += 1

        if call_count == 2:
            raise RuntimeError(
                'simulated write failure'
            )

        original_write(
            df,
            path,
        )

    monkeypatch.setattr(
        publication,
        '_write_parquet',
        failing_write,
    )

    with pytest.raises(
        RuntimeError,
        match='simulated write failure',
    ):
        publish_sales_run(
            result=_result(
                spark,
                price='15.00',
            ),
            output_root=output_root,
            quarantine_root=quarantine_root,
            run_id='run-002',
        )

    # The failed run must not become authoritative.
    assert (
        get_current_run_id(output_root)
        == 'run-001'
    )

    current_df, _ = load_current_sales_state(
        spark,
        output_root,
        quarantine_root,
    )

    assert (
        current_df.first()['unit_price']
        == Decimal('10.00')
    )
