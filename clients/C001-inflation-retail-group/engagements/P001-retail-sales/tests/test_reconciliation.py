"""
Tests for P001 pre-publication sales reconciliation.
"""
import pytest
from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from p001_retail_sales.reconciliation import (
    ReconciliationError,
    assert_sales_reconciliation,
    reconcile_sales_run,
)
from p001_retail_sales.transformations import add_sales_metrics


RESOLVED_TEST_SCHEMA = StructType([
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


def _make_resolved_df(spark):
    """
    Create a small valid resolved current state.
    """
    return spark.createDataFrame(
        [
            (
                'O2001',
                1,
                2,
                Decimal('10.00'),
                Decimal('6.00'),
                Decimal('1.00'),
                'COMPLETED',
                [],
            ),
            (
                'O2002',
                1,
                1,
                Decimal('20.00'),
                Decimal('12.00'),
                Decimal('5.00'),
                'CANCELLED',
                [],
            ),
        ],
        schema=RESOLVED_TEST_SCHEMA,
    )


def _empty_like(df):
    """
    Return an empty DataFrame with the same schema.
    """
    return df.limit(0)


def _reconcile_valid_run(spark):
    """
    Build the normal inputs for a successful reconciliation.
    """
    resolved_df = _make_resolved_df(spark)

    validated_df = resolved_df
    accepted_df = resolved_df
    validation_quarantine_df = _empty_like(
        resolved_df
    )
    ambiguous_df = _empty_like(
        resolved_df
    )

    candidate_df = add_sales_metrics(
        resolved_df
    )

    failures = reconcile_sales_run(
        validated_incoming_df=validated_df,
        accepted_incoming_df=accepted_df,
        validation_quarantine_df=validation_quarantine_df,
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=ambiguous_df,
    )

    return (
        failures,
        resolved_df,
        validated_df,
        accepted_df,
        validation_quarantine_df,
        candidate_df,
        ambiguous_df,
    )


# =============================================================================
# SUCCESSFUL RECONCILIATION
# =============================================================================


def test_reconciliation_accepts_valid_candidate(spark):
    (
        failures,
        _,
        _,
        _,
        _,
        _,
        _,
    ) = _reconcile_valid_run(spark)

    assert failures == []


def test_reconciliation_assertion_accepts_valid_candidate(spark):
    (
        _,
        resolved_df,
        validated_df,
        accepted_df,
        validation_quarantine_df,
        candidate_df,
        ambiguous_df,
    ) = _reconcile_valid_run(spark)

    # Should not raise.
    assert_sales_reconciliation(
        validated_incoming_df=validated_df,
        accepted_incoming_df=accepted_df,
        validation_quarantine_df=validation_quarantine_df,
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=ambiguous_df,
    )


# =============================================================================
# VALIDATION PARTITION
# =============================================================================


def test_reconciliation_detects_validation_partition_mismatch(spark):
    resolved_df = _make_resolved_df(spark)

    validated_df = resolved_df
    accepted_df = resolved_df.limit(1)

    # One validated row is deliberately missing from both outputs.
    validation_quarantine_df = _empty_like(
        resolved_df
    )

    candidate_df = add_sales_metrics(
        resolved_df
    )

    failures = reconcile_sales_run(
        validated_incoming_df=validated_df,
        accepted_incoming_df=accepted_df,
        validation_quarantine_df=validation_quarantine_df,
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=_empty_like(resolved_df),
    )

    assert (
        'VALIDATION_PARTITION_COUNT_MISMATCH'
        in failures
    )


# =============================================================================
# DUPLICATE TRUSTED BUSINESS KEYS
# =============================================================================


def test_reconciliation_detects_duplicate_candidate_key(spark):
    resolved_df = _make_resolved_df(spark)

    candidate_df = add_sales_metrics(
        resolved_df
    )

    duplicated_candidate_df = candidate_df.unionByName(
        candidate_df.filter(
            F.col('order_id') == 'O2001'
        )
    )

    failures = reconcile_sales_run(
        validated_incoming_df=resolved_df,
        accepted_incoming_df=resolved_df,
        validation_quarantine_df=_empty_like(resolved_df),
        resolved_state_df=resolved_df,
        candidate_df=duplicated_candidate_df,
        ambiguous_df=_empty_like(resolved_df),
    )

    assert (
        'DUPLICATE_CURATED_BUSINESS_KEY'
        in failures
    )


# =============================================================================
# REJECTED RECORD LEAKAGE
# =============================================================================


def test_reconciliation_detects_rejected_record_in_candidate(spark):
    resolved_df = _make_resolved_df(spark)

    candidate_df = (
        add_sales_metrics(resolved_df)
        .withColumn(
            'rejection_reasons',
            F.when(
                F.col('order_id') == 'O2001',
                F.array(
                    F.lit('UNKNOWN_PRODUCT_ID')
                ),
            ).otherwise(
                F.col('rejection_reasons')
            ),
        )
    )

    failures = reconcile_sales_run(
        validated_incoming_df=resolved_df,
        accepted_incoming_df=resolved_df,
        validation_quarantine_df=_empty_like(resolved_df),
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=_empty_like(resolved_df),
    )

    assert (
        'REJECTED_RECORD_IN_CANDIDATE'
        in failures
    )


# =============================================================================
# AMBIGUOUS VERSION ISOLATION
# =============================================================================


def test_reconciliation_detects_ambiguous_key_in_candidate(spark):
    resolved_df = _make_resolved_df(spark)

    candidate_df = add_sales_metrics(
        resolved_df
    )

    ambiguous_df = (
        resolved_df
        .filter(
            F.col('order_id') == 'O2001'
        )
        .withColumn(
            'rejection_reasons',
            F.array(
                F.lit(
                    'AMBIGUOUS_LATEST_VERSION'
                )
            ),
        )
    )

    failures = reconcile_sales_run(
        validated_incoming_df=resolved_df,
        accepted_incoming_df=resolved_df,
        validation_quarantine_df=_empty_like(resolved_df),
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=ambiguous_df,
    )

    assert (
        'AMBIGUOUS_KEY_IN_CANDIDATE'
        in failures
    )


def test_reconciliation_allows_unrelated_ambiguous_rows(spark):
    resolved_df = _make_resolved_df(spark)

    candidate_df = add_sales_metrics(
        resolved_df
    )

    ambiguous_df = spark.createDataFrame(
        [
            (
                'O9999',
                1,
                1,
                Decimal('50.00'),
                Decimal('30.00'),
                Decimal('0.00'),
                'COMPLETED',
                ['AMBIGUOUS_LATEST_VERSION'],
            ),
        ],
        schema=RESOLVED_TEST_SCHEMA,
    )

    failures = reconcile_sales_run(
        validated_incoming_df=resolved_df,
        accepted_incoming_df=resolved_df,
        validation_quarantine_df=_empty_like(resolved_df),
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=ambiguous_df,
    )

    assert (
        'AMBIGUOUS_KEY_IN_CANDIDATE'
        not in failures
    )


# =============================================================================
# TRANSFORMATION ROW PRESERVATION
# =============================================================================


def test_reconciliation_detects_missing_transformed_key(spark):
    resolved_df = _make_resolved_df(spark)

    candidate_df = (
        add_sales_metrics(resolved_df)
        .filter(
            F.col('order_id') != 'O2002'
        )
    )

    failures = reconcile_sales_run(
        validated_incoming_df=resolved_df,
        accepted_incoming_df=resolved_df,
        validation_quarantine_df=_empty_like(resolved_df),
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=_empty_like(resolved_df),
    )

    assert (
        'TRANSFORMED_KEY_SET_MISMATCH'
        in failures
    )


# =============================================================================
# METRIC CONSISTENCY
# =============================================================================


def test_reconciliation_detects_incorrect_sales_metric(spark):
    resolved_df = _make_resolved_df(spark)

    candidate_df = (
        add_sales_metrics(resolved_df)
        .withColumn(
            'gross_sales',
            F.when(
                F.col('order_id') == 'O2001',
                F.lit('999.00').cast(
                    'decimal(24,2)'
                ),
            ).otherwise(
                F.col('gross_sales')
            ),
        )
    )

    failures = reconcile_sales_run(
        validated_incoming_df=resolved_df,
        accepted_incoming_df=resolved_df,
        validation_quarantine_df=_empty_like(resolved_df),
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=_empty_like(resolved_df),
    )

    assert (
        'INCONSISTENT_SALES_METRICS'
        in failures
    )


def test_reconciliation_detects_nonzero_cancelled_metrics(spark):
    resolved_df = _make_resolved_df(spark)

    candidate_df = (
        add_sales_metrics(resolved_df)
        .withColumn(
            'net_sales',
            F.when(
                F.col('order_status') == 'CANCELLED',
                F.lit('5.00').cast(
                    'decimal(24,2)'
                ),
            ).otherwise(
                F.col('net_sales')
            ),
        )
    )

    failures = reconcile_sales_run(
        validated_incoming_df=resolved_df,
        accepted_incoming_df=resolved_df,
        validation_quarantine_df=_empty_like(resolved_df),
        resolved_state_df=resolved_df,
        candidate_df=candidate_df,
        ambiguous_df=_empty_like(resolved_df),
    )

    assert (
        'INCONSISTENT_SALES_METRICS'
        in failures
    )


# =============================================================================
# PUBLICATION BLOCKING
# =============================================================================


def test_reconciliation_assertion_raises_on_failure(spark):
    resolved_df = _make_resolved_df(spark)

    candidate_df = (
        add_sales_metrics(resolved_df)
        .unionByName(
            add_sales_metrics(
                resolved_df.filter(
                    F.col('order_id') == 'O2001'
                )
            )
        )
    )

    with pytest.raises(
        ReconciliationError,
        match='DUPLICATE_CURATED_BUSINESS_KEY',
    ):
        assert_sales_reconciliation(
            validated_incoming_df=resolved_df,
            accepted_incoming_df=resolved_df,
            validation_quarantine_df=_empty_like(resolved_df),
            resolved_state_df=resolved_df,
            candidate_df=candidate_df,
            ambiguous_df=_empty_like(resolved_df),
        )
