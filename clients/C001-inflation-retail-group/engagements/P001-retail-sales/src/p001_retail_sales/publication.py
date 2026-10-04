"""
Local Parquet persistence and safe publication for P001.

The resulting local structure will look like:

    data/dev/
    ├── output/
    │   ├── CURRENT
    │   ├── _staging/
    │   └── runs/
    │       ├── run-001/
    │       │   ├── resolved_state/
    │       │   └── curated_sales/
    │       └── run-002/
    │           ├── resolved_state/
    │           └── curated_sales/
    │
    └── quarantine/
        ├── _staging/
        └── runs/
            ├── run-001/
            │   ├── ambiguous_state/
            │   ├── sales_validation/
            │   ├── products/
            │   └── stores/
            └── run-002/
                └── ...

Key Idea: `CURRENT` contains only something like `run-002`.
          So, publishing does NOT overwrite yesterday's trusted Parquet data.

          We first create an entirely new snapshot: `runs/run-002`,
          and only after everything succeeds do we atomically change:
              CURRENT
              run-001
          to:
              CURRENT
              run-002
        
          If something crashes while writing `run-002`, `CURRENT` still says
          `run-001`. Therefore, consumers continue to see the last known-good
          state.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

if TYPE_CHECKING:
    from p001_retail_sales.pipeline import SalesPipelineResult


class PublicationError(RuntimeError):
    """
    Raised when local pipeline state cannot be safely published or loaded.
    """


def _write_parquet(df: DataFrame, path: Path) -> None:
    """
    Write one DataFrame as a Parquet dataset.
    """
    df.write.mode('overwrite').parquet(str(path))


def _remove_if_exists(path: Path) -> None:
    """
    Remove a generated directory if it exists.
    """
    if path.exists():
        shutil.rmtree(path)


def _write_current_pointer(output_root: Path, run_id: str) -> None:
    """
    Atomically update the CURRENT pointer to a successful run.

    The pointer is changed only after every dataset for the run
    has been written successfully.
    """
    temp_pointer = output_root / '.CURRENT.tmp'
    curr_pointer = output_root / 'CURRENT'

    temp_pointer.write_text(f'{run_id}\n', encoding='utf-8')
    os.replace(temp_pointer, curr_pointer)


def publish_sales_run(
    result: SalesPipelineResult,
    output_root: str | Path,
    quarantine_root: str | Path,
    run_id: str,
) -> None:
    """
    Persist one successfully reconciled P001 run.

    Publication uses immutable run snapshots plus a small CURRENT pointer.

    A new run becomes authoritative only after all Parquet datasets
    have been written successfully.

    If publication failes before CURRENT is updated, the previous
    successful run remains authoritative.
    """
    output_root = Path(output_root)
    quarantine_root = Path(quarantine_root)

    output_staging_path = output_root / '_staging' / run_id
    quarantine_staging_path = quarantine_root / '_staging' / run_id

    output_run_path = output_root / 'runs' / run_id
    quarantine_run_path = quarantine_root / 'runs' / run_id

    if output_run_path.exists() or quarantine_run_path.exists():
        raise PublicationError(f'Run already exists: {run_id}')

    # Clean up any abandoned staging directories left by an earlier
    # failed attempt using the same run_id.
    _remove_if_exists(output_staging_path)
    _remove_if_exists(quarantine_staging_path)

    output_staging_path.parent.mkdir(parents=True, exist_ok=True)
    quarantine_staging_path.parent.mkdir(parents=True, exist_ok=True)

    output_run_path.parent.mkdir(parents=True, exist_ok=True)
    quarantine_run_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # ---------------------------------------------------------------------
        # TRUSTED / INTERNAL STATE
        # ---------------------------------------------------------------------
        _write_parquet(
            result.resolved_state_df,
            output_staging_path / 'resolved_state',
        )
        _write_parquet(
            result.candidate_df,
            output_staging_path / 'curated_sales',
        )

        # ---------------------------------------------------------------------
        # EXCEPTION / QUARANTINE STATE
        # ---------------------------------------------------------------------
        _write_parquet(
            result.products_quarantine_df,
            quarantine_staging_path / 'products',
        )
        _write_parquet(
            result.stores_quarantine_df,
            quarantine_staging_path / 'stores',
        )
        _write_parquet(
            result.validation_quarantine_df,
            quarantine_staging_path / 'sales_validation',
        )
        # Ambiguity is persisted because unresolved conflicts must be
        # remembered by the next incremental run.
        _write_parquet(
            result.ambiguous_state_df,
            quarantine_staging_path / 'ambiguous_state',
        )

        # ---------------------------------------------------------------------
        # PROMOTE STAGED SNAPSHOT
        # ---------------------------------------------------------------------
        output_staging_path.rename(output_run_path)
        quarantine_staging_path.rename(quarantine_run_path)

        # Only now does the new run become authoritative.
        _write_current_pointer(output_root, run_id)

    except Exception:
        # A failed run must NOT disturb the previous CURRENT pointer.
        _remove_if_exists(output_staging_path)
        _remove_if_exists(quarantine_staging_path)

        # These directories may already have been renamed from staging
        # before a later publication step failed. Because CURRENT has
        # not yet been changed, they are safe to remove.
        current_run_id = None
        current_pointer = output_root / 'CURRENT'
        if current_pointer.exists():
            current_run_id = (
                current_pointer
                .read_text(encoding='utf-8')
                .strip()
            )
        if current_run_id != run_id:
            _remove_if_exists(output_run_path)
            _remove_if_exists(quarantine_run_path)

        raise


def get_current_run_id(output_root: str | Path) -> str | None:
    """
    Return the currently published run ID.

    None means no successful run has been published yet.
    """
    current_pointer = Path(output_root) / 'CURRENT'

    if not current_pointer.exists():
        return None

    run_id = current_pointer.read_text(encoding='utf-8').strip()

    if not run_id:
        raise PublicationError('CURRENT pointer is empty.')

    return run_id


def load_current_sales_state(
    spark: SparkSession,
    output_root: str | Path,
    quarantine_root: str | Path,
) -> tuple[DataFrame | None, DataFrame | None]:
    """
    Load the state required for the next incremental run.

    Returns:
        current_state_df:
            Previously resolved trusted canonical state.
        
        ambiguous_state_df:
            Previously unresolved latest-version conflicts.
    
    If no successful run exists, both values are None.
    """
    output_root = Path(output_root)
    quarantine_root = Path(quarantine_root)

    run_id = get_current_run_id(output_root)
    if run_id is None:
        return None, None

    resolved_state_path = output_root / 'runs' / run_id / 'resolved_state'
    if not resolved_state_path.exists():
        raise PublicationError(
            f'Published resolved state is missing for run {run_id}.'
        )

    ambiguous_state_path = quarantine_root / 'runs' / run_id / 'ambiguous_state'
    if not ambiguous_state_path.exists():
        raise PublicationError(
            f'Published ambiguous state is missing for run {run_id}.'
        )

    current_state_df = spark.read.parquet(str(resolved_state_path))
    ambiguous_state_df = spark.read.parquet(str(ambiguous_state_path))

    return current_state_df, ambiguous_state_df


def load_current_curated_sales(
    spark: SparkSession,
    output_root: str | Path,
) -> DataFrame | None:
    """
    Load the currently published analytical sales dataset.
    """
    output_root = Path(output_root)

    run_id = get_current_run_id(output_root)
    if run_id is None:
        return None
    
    curated_path = output_root / 'runs' / run_id / 'curated_sales'
    if not curated_path.exists():
        raise PublicationError(
            f'Published curated sales are missing for run {run_id}.'
        )

    return spark.read.parquet(str(curated_path))
