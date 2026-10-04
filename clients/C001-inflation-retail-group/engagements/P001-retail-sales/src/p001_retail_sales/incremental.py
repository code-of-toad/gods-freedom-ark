"""
Incremental current-state processing for P001 sales.
"""
from pyspark.sql import DataFrame

from p001_retail_sales.resolution import resolve_sales_versions


def merge_sales_current_state(
    current_df: DataFrame | None,
    incoming_df: DataFrame,
    ambiguous_state_df: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """
    Merge accepted incoming sales records with prior resolution state.

    Args:
        current_df:
            Previously resolved trusted current state.

        incoming_df:
            Newly accepted sales records from the current batch.

        ambiguous_state_df:
            Previously unresolved latest-version conflicts.

    Returns:
        next_state_df:
            Authoritative current state after incorporating the batch.

        ambiguous_df:
            Unresolved latest-version conflicts that must remain
            outside trusted analytical state.
    """
    combined_df = incoming_df

    if current_df is not None:
        combined_df = current_df.unionByName(combined_df)

    if ambiguous_state_df is not None:
        combined_df = ambiguous_state_df.unionByName(combined_df)

    next_state_df, ambiguous_df = resolve_sales_versions(combined_df)

    return next_state_df, ambiguous_df
