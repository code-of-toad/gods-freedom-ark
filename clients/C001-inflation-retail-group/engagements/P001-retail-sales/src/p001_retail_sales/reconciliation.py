"""
Pre-publication reconciliation checks for P001 sales.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from p001_retail_sales.resolution import SALES_BUSINESS_KEY
from p001_retail_sales.transformations import SALES_METRIC_TYPE


SALES_METRIC_COLUMNS = {
    'gross_sales',
    'net_sales',
    'gross_margin',
}


class ReconciliationError(RuntimeError):
    """
    Raised when a candidate trusted state fails reconciliation.
    """


def reconcile_sales_run(
    validated_incoming_df: DataFrame,
    accepted_incoming_df: DataFrame,
    validation_quarantine_df: DataFrame,
    resolved_state_df: DataFrame,
    candidate_df: DataFrame,
    ambiguous_df: DataFrame,
) -> list[str]:
    """
    Reconcile a candidate P001 sales state before publication.

    Returns a list of failure codes.

    An empty list means that the candidate passed reconciliation.

    Args:
        validated_incoming_df:
            Incoming batch after complete validation.

        accepted_incoming_df:
            Incoming rows that passed validation.

        validation_quarantine_df:
            Incoming rows rejected during validation.

        resolved_state_df:
            Authoritative current state after incremental/version
            resolution but before business transformations.

        candidate_df:
            Transformed candidate trusted state proposed for publication.

        ambiguous_df:
            Latest-version conflicts routed to the exception flow.
    """
    failures = []

    # -------------------------------------------------------------------------
    # VALIDATION PARTITION
    # -------------------------------------------------------------------------

    # Validation must account for every incoming row exactly once:
    #
    # validated input = accepted + validation quarantine
    if (
        validated_incoming_df.count()
        != accepted_incoming_df.count()
        + validation_quarantine_df.count()
    ):
        failures.append(
            'VALIDATION_PARTITION_COUNT_MISMATCH'
        )

    # -------------------------------------------------------------------------
    # CURATED BUSINESS-KEY UNIQUENESS
    # -------------------------------------------------------------------------

    duplicate_key_exists = (
        candidate_df
        .groupBy(*SALES_BUSINESS_KEY)
        .count()
        .filter(F.col('count') > 1)
        .limit(1)
        .count()
        > 0
    )

    if duplicate_key_exists:
        failures.append(
            'DUPLICATE_CURATED_BUSINESS_KEY'
        )

    # -------------------------------------------------------------------------
    # REJECTED RECORDS MUST NOT ENTER TRUSTED STATE
    # -------------------------------------------------------------------------

    rejected_record_exists = (
        candidate_df
        .filter(
            F.size(F.col('rejection_reasons')) > 0
        )
        .limit(1)
        .count()
        > 0
    )

    if rejected_record_exists:
        failures.append(
            'REJECTED_RECORD_IN_CANDIDATE'
        )

    # -------------------------------------------------------------------------
    # AMBIGUOUS KEYS MUST NOT ENTER TRUSTED STATE
    # -------------------------------------------------------------------------

    ambiguous_keys_df = (
        ambiguous_df
        .select(*SALES_BUSINESS_KEY)
        .distinct()
    )

    ambiguous_key_in_candidate = (
        candidate_df
        .select(*SALES_BUSINESS_KEY)
        .join(
            ambiguous_keys_df,
            on=SALES_BUSINESS_KEY,
            how='inner',
        )
        .limit(1)
        .count()
        > 0
    )

    if ambiguous_key_in_candidate:
        failures.append(
            'AMBIGUOUS_KEY_IN_CANDIDATE'
        )

    # -------------------------------------------------------------------------
    # TRANSFORMATION MUST PRESERVE RESOLVED BUSINESS KEYS
    # -------------------------------------------------------------------------

    resolved_keys_df = (
        resolved_state_df
        .select(*SALES_BUSINESS_KEY)
        .distinct()
    )

    candidate_keys_df = (
        candidate_df
        .select(*SALES_BUSINESS_KEY)
        .distinct()
    )

    missing_candidate_key = (
        resolved_keys_df
        .join(
            candidate_keys_df,
            on=SALES_BUSINESS_KEY,
            how='left_anti',
        )
        .limit(1)
        .count()
        > 0
    )

    unexpected_candidate_key = (
        candidate_keys_df
        .join(
            resolved_keys_df,
            on=SALES_BUSINESS_KEY,
            how='left_anti',
        )
        .limit(1)
        .count()
        > 0
    )

    if missing_candidate_key or unexpected_candidate_key:
        failures.append(
            'TRANSFORMED_KEY_SET_MISMATCH'
        )

    # -------------------------------------------------------------------------
    # REQUIRED ANALYTICAL METRICS
    # -------------------------------------------------------------------------

    if not SALES_METRIC_COLUMNS.issubset(
        set(candidate_df.columns)
    ):
        failures.append(
            'MISSING_SALES_METRIC_COLUMNS'
        )

        # Metric consistency cannot be checked without the columns.
        return failures

    # -------------------------------------------------------------------------
    # DERIVED-METRIC CONSISTENCY
    # -------------------------------------------------------------------------

    zero = F.lit(0).cast(SALES_METRIC_TYPE)

    calculated_gross_sales = (
        F.col('quantity')
        * F.col('unit_price')
    ).cast(SALES_METRIC_TYPE)

    calculated_line_cost = (
        F.col('quantity')
        * F.col('unit_cost')
    ).cast(SALES_METRIC_TYPE)

    expected_gross_sales = (
        F.when(
            F.col('order_status') == 'CANCELLED',
            zero,
        )
        .otherwise(
            calculated_gross_sales
        )
    )

    expected_net_sales = (
        F.when(
            F.col('order_status') == 'CANCELLED',
            zero,
        )
        .otherwise(
            (
                calculated_gross_sales
                - F.col('discount_amount')
            ).cast(SALES_METRIC_TYPE)
        )
    )

    expected_gross_margin = (
        F.when(
            F.col('order_status') == 'CANCELLED',
            zero,
        )
        .otherwise(
            (
                expected_net_sales
                - calculated_line_cost
            ).cast(SALES_METRIC_TYPE)
        )
    )

    inconsistent_metric_exists = (
        candidate_df
        .filter(
            F.col('gross_sales').isNull()
            | F.col('net_sales').isNull()
            | F.col('gross_margin').isNull()
            | (
                F.col('gross_sales')
                != expected_gross_sales
            )
            | (
                F.col('net_sales')
                != expected_net_sales
            )
            | (
                F.col('gross_margin')
                != expected_gross_margin
            )
        )
        .limit(1)
        .count()
        > 0
    )

    if inconsistent_metric_exists:
        failures.append(
            'INCONSISTENT_SALES_METRICS'
        )

    return failures


def assert_sales_reconciliation(
    validated_incoming_df: DataFrame,
    accepted_incoming_df: DataFrame,
    validation_quarantine_df: DataFrame,
    resolved_state_df: DataFrame,
    candidate_df: DataFrame,
    ambiguous_df: DataFrame,
) -> None:
    """
    Raise ReconciliationError when a candidate state is not safe
    to publish.
    """
    failures = reconcile_sales_run(
        validated_incoming_df=validated_incoming_df,
        accepted_incoming_df=accepted_incoming_df,
        validation_quarantine_df=validation_quarantine_df,
        resolved_state_df=resolved_state_df,
        candidate_df=candidate_df,
        ambiguous_df=ambiguous_df,
    )

    if failures:
        raise ReconciliationError(
            'Sales reconciliation failed: '
            + ', '.join(failures)
        )
