"""
End-to-end P001 pipeline orchestration.
"""
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from p001_retail_sales.ingestion import (
    read_raw_products,
    read_raw_sales,
    read_raw_stores,
)
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
from p001_retail_sales.transformations import add_sales_metrics
from p001_retail_sales.incremental import merge_sales_current_state
from p001_retail_sales.reconciliation import assert_sales_reconciliation


@dataclass(frozen=True)
class SalesPipelineResult:
    """
    Outputs produced by one successful P001 sales pipeline run.

    resolved_state_df:
        Canonical current state used as input to the next incremental run.

    candidate_df:
        Reconciled analytical state eligible for publication.

    validation_quarantine_df
        Incoming sales rows that failed validation.

    ambiguous_state_df
        Persistent unresolved version conflicts.

    products_quarantine_df, stores_quarantine_df:
        Invalid reference-data rows retained for investigation.
    """
    resolved_state_df: DataFrame
    candidate_df: DataFrame
    validation_quarantine_df: DataFrame
    ambiguous_state_df: DataFrame
    products_quarantine_df: DataFrame
    stores_quarantine_df: DataFrame


class ReferenceDataError(RuntimeError):
    """
    Raised when reference-data corruption makes trusted processing unsafe.
    """


def _contains_rejection_reason(
    df: DataFrame,
    reason: str,
) -> bool:
    """
    Return True when at least one row contains the specified reason.
    """
    return (
        df
        .filter(
            F.array_contains(
                F.col('rejection_reasons'),
                reason,
            )
        )
        .limit(1)
        .count()
        > 0
    )


def _assert_reference_data_is_processable(
    products_df: DataFrame,
    stores_df: DataFrame,
) -> None:
    """
    Fail the run for reference-data conditions that make identity
    ambiguous.

    Other invalid reference rows may still be quarantined individually.
    """
    failures = []

    if _contains_rejection_reason(
        products_df,
        'DUPLICATE_PRODUCT_ID',
    ):
        failures.append('DUPLICATE_PRODUCT_ID')

    if _contains_rejection_reason(
        stores_df,
        'DUPLICATE_STORE_ID',
    ):
        failures.append('DUPLICATE_STORE_ID')

    if failures:
        raise ReferenceDataError(
            'Reference data validation failed: ' + ', '.join(failures)
        )


def run_sales_pipeline(
    spark: SparkSession,
    sales_path: str,
    products_path: str,
    stores_path: str,
    current_state_df: DataFrame | None = None,
    ambiguous_state_df: DataFrame | None = None,
) -> SalesPipelineResult:
    """
    Execute one P001 incremental sales-processing run.

    Args:
        spark:
            Active Spark session.

        sales_path:
            Incoming raw sales CSV path for this batch.

        products_path:
            Raw canonical product-reference CSV path.

        stores_path:
            Raw canonical store-reference CSV path.

        current_state_df:
            Previously resolved canonical sales state.
            None represents the first run.

        ambiguous_state_df:
            Previously unresolved version conflicts.
            None represents no prior ambiguity.

    Returns:
        SalesPipelineResult containing reconciled candidate output,
        next incremental state, and exception outputs.

    Raises:
        ReferenceDataError:
            Reference-data identity is unsafe for processing.

        ReconciliationError:
            Candidate trusted output fails reconciliation.
    """
    # =========================================================================
    # REFERENCE DATA
    # =========================================================================
    raw_products_df = read_raw_products(
        spark,
        products_path,
    )
    products_validated_df = add_products_rejection_reasons(
        standardize_products(raw_products_df)
    )

    raw_stores_df = read_raw_stores(
        spark,
        stores_path,
    )
    stores_validated_df = add_stores_rejection_reasons(
        standardize_stores(raw_stores_df)
    )

    # Duplicate reference identities make deterministic lookup unsafe.
    _assert_reference_data_is_processable(
        products_validated_df,
        stores_validated_df,
    )

    products_accepted_df, products_quarantine_df = (
        split_validated_records(products_validated_df)
    )

    stores_accepted_df, stores_quarantine_df = (
        split_validated_records(stores_validated_df)
    )

    # =========================================================================
    # SALES INGESTION + STANDARDIZATION
    # =========================================================================
    raw_sales_df = read_raw_sales(
        spark,
        sales_path,
    )
    standardized_sales_df = standardize_sales(raw_sales_df)

    # =========================================================================
    # SALES VALIDATION
    # =========================================================================
    validated_sales_df = validate_sales(
        standardized_sales_df,
        products_accepted_df,
        stores_accepted_df,
    )

    accepted_sales_df, validation_quarantine_df = (
        split_validated_records(validated_sales_df)
    )

    # =========================================================================
    # INCREMENTAL / VERSION RESOLUTION
    # =========================================================================
    resolved_state_df, next_ambiguous_state_df = (
        merge_sales_current_state(
            current_df=current_state_df,
            incoming_df=accepted_sales_df,
            ambiguous_state_df=ambiguous_state_df,
        )
    )

    # =========================================================================
    # BUSINESS TRANSFORMATIONS
    # =========================================================================
    candidate_df = add_sales_metrics(resolved_state_df)

    # =========================================================================
    # PRE-PUBLICATION RECONCILIATION
    # =========================================================================
    assert_sales_reconciliation(
        validated_incoming_df=validated_sales_df,
        accepted_incoming_df=accepted_sales_df,
        validation_quarantine_df=validation_quarantine_df,
        resolved_state_df=resolved_state_df,
        candidate_df=candidate_df,
        ambiguous_df=next_ambiguous_state_df,
    )

    # Nothing has been physically published yet.
    # Reaching this point means candidate_df is eligible for publication.
    return SalesPipelineResult(
        resolved_state_df=resolved_state_df,
        candidate_df=candidate_df,
        validation_quarantine_df=validation_quarantine_df,
        ambiguous_state_df=next_ambiguous_state_df,
        products_quarantine_df=products_quarantine_df,
        stores_quarantine_df=stores_quarantine_df,
    )
