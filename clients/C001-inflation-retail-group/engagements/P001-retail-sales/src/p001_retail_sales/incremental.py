"""
Incremental current-state processing for P001 sales.
"""
from pyspark.sql import DataFrame

from p001_retail_sales.resolution import resolve_sales_versions


def merge_sales_current_state(
    current_df: DataFrame | None,
    incoming_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Merge accepted incoming sales records with the previously trusted
    current state.

    Args:
        current_df:
            Previously resolved current sales state.
            None represents the first pipeline run.

        incoming_df:
            Newly accepted sales records from the current ingestion batch.

    Returns:
        next_state_df:
            Authoritative current state after incorporating the batch.

        ambiguous_df:
            Latest-version conflicts that cannot be resolved
            deterministically.
    """
    # On the first pipeline run there is no previous trusted state.
    if current_df is None:
        combined_df = incoming_df
    else:
        combined_df = current_df.unionByName(incoming_df)

    # Reuse the same resolution rules regardless of whether a conflict,
    # correction, replay, or stale version came from the current batch
    # or from previously trusted state.
    next_state_df, ambiguous_df = resolve_sales_versions(
        combined_df
    )

    return next_state_df, ambiguous_df
