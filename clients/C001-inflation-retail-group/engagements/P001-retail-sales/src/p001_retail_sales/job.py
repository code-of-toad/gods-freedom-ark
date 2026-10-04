"""
Executable batch-job coordination for P001 sales.
"""
from pathlib import Path
from pyspark.sql import SparkSession

from p001_retail_sales.pipeline import (
    SalesPipelineResult,
    run_sales_pipeline,
)
from p001_retail_sales.publication import (
    load_current_sales_state,
    publish_sales_run,
)


def run_sales_batch_job(
    spark: SparkSession,
    sales_path: str | Path,
    products_path: str | Path,
    stores_path: str | Path,
    output_root: str | Path,
    quarantine_root: str | Path,
    run_id: str,
) -> SalesPipelineResult:
    """
    Execute and publish one complete incremental P001 sales batch.

    Workflow:
        1. Load the last successfully published state.
        2. Process the incoming sales batch.
        3. Reconcile the candidate output.
        4. Publish a new immutable Parquet snapshot.
        5. Update CURRENT only after publication succeeds.

    If processing or publication fails, the previously published
    CURRENT run remains authoritative.
    """
    # -------------------------------------------------------------------------
    # LOAD PRIOR INCREMENTAL STATE
    # -------------------------------------------------------------------------
    current_state_df, ambiguous_state_df = (
        load_current_sales_state(
            spark=spark,
            output_root=output_root,
            quarantine_root=quarantine_root,
        )
    )

    # -------------------------------------------------------------------------
    # PROCESS INCOMING BATCH
    # -------------------------------------------------------------------------
    result = run_sales_pipeline(
        spark=spark,
        sales_path=str(sales_path),
        products_path=str(products_path),
        stores_path=str(stores_path),
        current_state_df=current_state_df,
        ambiguous_state_df=ambiguous_state_df,
    )

    # -------------------------------------------------------------------------
    # PUBLISH SUCCESSFUL RESULT
    # -------------------------------------------------------------------------
    publish_sales_run(
        result=result,
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id=run_id,
    )

    return result
