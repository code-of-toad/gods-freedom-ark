"""
Parquet persistence and safe publication for P001.

Local filesystems use a staging-directory rename before CURRENT is updated.

URI/object-storage locations (for example gs://...) write the immutable run
snapshot directly to its final run prefix, then update CURRENT only after all
datasets succeed. This avoids an expensive object-store "rename", which is
typically implemented as copy + delete rather than a filesystem metadata move.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

from p001_retail_sales.config import (
    is_uri_location,
    join_location,
)

if TYPE_CHECKING:
    from p001_retail_sales.pipeline import SalesPipelineResult


ANALYTICAL_TABLE_NAMES = {
    'fact_sales',
    'dim_product',
    'dim_store',
    'dim_date',
}


class PublicationError(RuntimeError):
    """
    Raised when pipeline state cannot be safely published or loaded.
    """


def _write_parquet(
    df: DataFrame,
    location: str | Path,
) -> None:
    """
    Write one DataFrame as a Parquet dataset.
    """
    df.write.mode('overwrite').parquet(str(location))


def _active_spark(
    spark: SparkSession | None = None,
) -> SparkSession:
    """
    Return the supplied/active Spark session for URI filesystem operations.
    """
    if spark is not None:
        return spark

    active = SparkSession.getActiveSession()

    if active is None:
        raise PublicationError(
            'A Spark session is required for URI-based publication.'
        )

    return active


def _hadoop_filesystem(
    spark: SparkSession,
    location: str,
):
    """
    Return Hadoop FileSystem + Path objects for a URI location.

    Managed Spark supplies the GCS connector, so gs:// locations can be
    inspected without adding a separate storage SDK dependency here.
    """
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(location)

    filesystem = path.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )

    return filesystem, path


def _remote_exists(
    spark: SparkSession,
    location: str,
) -> bool:
    filesystem, path = _hadoop_filesystem(
        spark,
        location,
    )

    return bool(filesystem.exists(path))


def _remove_remote_if_exists(
    spark: SparkSession,
    location: str,
) -> None:
    filesystem, path = _hadoop_filesystem(
        spark,
        location,
    )

    if filesystem.exists(path):
        filesystem.delete(
            path,
            True,
        )


def _remove_local_if_exists(
    path: Path,
) -> None:
    """
    Remove a generated local directory if it exists.
    """
    if path.exists():
        shutil.rmtree(path)


def _write_current_pointer_local(
    output_root: Path,
    run_id: str,
) -> None:
    """
    Atomically replace CURRENT on a local filesystem.
    """
    output_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_pointer = output_root / '.CURRENT.tmp'
    current_pointer = output_root / 'CURRENT'

    temp_pointer.write_text(
        f'{run_id}\n',
        encoding='utf-8',
    )

    os.replace(
        temp_pointer,
        current_pointer,
    )


def _write_current_pointer_remote(
    spark: SparkSession,
    output_root: str,
    run_id: str,
) -> None:
    """
    Write the small CURRENT object after the run snapshot is complete.

    On object storage, run data is immutable and is written first. CURRENT is
    therefore the final publication action.
    """
    current_location = join_location(
        output_root,
        'CURRENT',
    )

    filesystem, path = _hadoop_filesystem(
        spark,
        current_location,
    )

    parent = path.getParent()

    if parent is not None:
        filesystem.mkdirs(parent)

    stream = filesystem.create(
        path,
        True,
    )

    writer = spark._jvm.java.io.OutputStreamWriter(
        stream,
        'UTF-8',
    )

    try:
        writer.write(
            f'{run_id}\n'
        )

    finally:
        writer.close()


def _read_current_pointer_remote(
    spark: SparkSession,
    output_root: str,
) -> str | None:
    current_location = join_location(
        output_root,
        'CURRENT',
    )

    filesystem, path = _hadoop_filesystem(
        spark,
        current_location,
    )

    if not filesystem.exists(path):
        return None

    stream = filesystem.open(path)

    reader = spark._jvm.java.io.BufferedReader(
        spark._jvm.java.io.InputStreamReader(
            stream,
            'UTF-8',
        )
    )

    try:
        line = reader.readLine()

    finally:
        reader.close()

    if line is None:
        raise PublicationError(
            'CURRENT pointer is empty.'
        )

    run_id = str(line).strip()

    if not run_id:
        raise PublicationError(
            'CURRENT pointer is empty.'
        )

    return run_id


def get_current_run_id(
    output_root: str | Path,
    spark: SparkSession | None = None,
) -> str | None:
    """
    Return the currently published run ID.

    None means no successful run has been published yet.
    """
    output_value = str(output_root)

    if is_uri_location(output_value):
        spark = _active_spark(spark)

        return _read_current_pointer_remote(
            spark,
            output_value,
        )

    current_pointer = (
        Path(output_root)
        / 'CURRENT'
    )

    if not current_pointer.exists():
        return None

    run_id = (
        current_pointer
        .read_text(encoding='utf-8')
        .strip()
    )

    if not run_id:
        raise PublicationError(
            'CURRENT pointer is empty.'
        )

    return run_id


def _assert_matching_storage_kind(
    output_root: str | Path,
    quarantine_root: str | Path,
) -> bool:
    """
    Ensure output and quarantine are both local or both URI-based.

    Returns:
        True when both are URI-based, otherwise False.
    """
    output_remote = is_uri_location(
        str(output_root)
    )

    quarantine_remote = is_uri_location(
        str(quarantine_root)
    )

    if output_remote != quarantine_remote:
        raise PublicationError(
            'output_root and quarantine_root must both be local '
            'or both be URI-based locations.'
        )

    return output_remote


def _publish_remote_sales_run(
    result: SalesPipelineResult,
    output_root: str,
    quarantine_root: str,
    run_id: str,
) -> None:
    """
    Publish one run directly to immutable object-storage run prefixes.

    We intentionally do not stage-and-rename on object storage because a
    directory rename can become a large copy/delete operation.
    """
    spark = result.resolved_state_df.sparkSession

    output_run = join_location(
        output_root,
        'runs',
        run_id,
    )

    quarantine_run = join_location(
        quarantine_root,
        'runs',
        run_id,
    )

    if (
        _remote_exists(
            spark,
            output_run,
        )
        or _remote_exists(
            spark,
            quarantine_run,
        )
    ):
        raise PublicationError(
            f'Run already exists: {run_id}'
        )

    current_updated = False

    try:
        # ---------------------------------------------------------------------
        # TRUSTED / INTERNAL STATE
        # ---------------------------------------------------------------------
        _write_parquet(
            result.resolved_state_df,
            join_location(
                output_run,
                'resolved_state',
            ),
        )

        _write_parquet(
            result.candidate_df,
            join_location(
                output_run,
                'curated_sales',
            ),
        )

        # ---------------------------------------------------------------------
        # ANALYTICAL MODEL
        # ---------------------------------------------------------------------
        analytical_root = join_location(
            output_run,
            'analytical',
        )

        _write_parquet(
            result.fact_sales_df,
            join_location(
                analytical_root,
                'fact_sales',
            ),
        )

        _write_parquet(
            result.dim_product_df,
            join_location(
                analytical_root,
                'dim_product',
            ),
        )

        _write_parquet(
            result.dim_store_df,
            join_location(
                analytical_root,
                'dim_store',
            ),
        )

        _write_parquet(
            result.dim_date_df,
            join_location(
                analytical_root,
                'dim_date',
            ),
        )

        # ---------------------------------------------------------------------
        # EXCEPTION / QUARANTINE STATE
        # ---------------------------------------------------------------------
        _write_parquet(
            result.products_quarantine_df,
            join_location(
                quarantine_run,
                'products',
            ),
        )

        _write_parquet(
            result.stores_quarantine_df,
            join_location(
                quarantine_run,
                'stores',
            ),
        )

        _write_parquet(
            result.validation_quarantine_df,
            join_location(
                quarantine_run,
                'sales_validation',
            ),
        )

        _write_parquet(
            result.ambiguous_state_df,
            join_location(
                quarantine_run,
                'ambiguous_state',
            ),
        )

        # Only after every immutable dataset succeeds does this run become
        # authoritative.
        _write_current_pointer_remote(
            spark,
            output_root,
            run_id,
        )

        current_updated = True

    except Exception:
        # Before CURRENT is changed, the previous run remains authoritative.
        # Remove partial prefixes so a failed physical run does not masquerade
        # as a complete historical snapshot.
        if not current_updated:
            _remove_remote_if_exists(
                spark,
                output_run,
            )

            _remove_remote_if_exists(
                spark,
                quarantine_run,
            )

        raise


def _publish_local_sales_run(
    result: SalesPipelineResult,
    output_root: Path,
    quarantine_root: Path,
    run_id: str,
) -> None:
    """
    Preserve the existing local stage -> rename -> CURRENT workflow.
    """
    output_staging = (
        output_root
        / '_staging'
        / run_id
    )

    quarantine_staging = (
        quarantine_root
        / '_staging'
        / run_id
    )

    output_run = (
        output_root
        / 'runs'
        / run_id
    )

    quarantine_run = (
        quarantine_root
        / 'runs'
        / run_id
    )

    if (
        output_run.exists()
        or quarantine_run.exists()
    ):
        raise PublicationError(
            f'Run already exists: {run_id}'
        )

    _remove_local_if_exists(
        output_staging
    )

    _remove_local_if_exists(
        quarantine_staging
    )

    output_staging.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_staging.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_run.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_run.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        _write_parquet(
            result.resolved_state_df,
            output_staging / 'resolved_state',
        )

        _write_parquet(
            result.candidate_df,
            output_staging / 'curated_sales',
        )

        analytical_root = (
            output_staging
            / 'analytical'
        )

        _write_parquet(
            result.fact_sales_df,
            analytical_root / 'fact_sales',
        )

        _write_parquet(
            result.dim_product_df,
            analytical_root / 'dim_product',
        )

        _write_parquet(
            result.dim_store_df,
            analytical_root / 'dim_store',
        )

        _write_parquet(
            result.dim_date_df,
            analytical_root / 'dim_date',
        )

        _write_parquet(
            result.products_quarantine_df,
            quarantine_staging / 'products',
        )

        _write_parquet(
            result.stores_quarantine_df,
            quarantine_staging / 'stores',
        )

        _write_parquet(
            result.validation_quarantine_df,
            quarantine_staging / 'sales_validation',
        )

        _write_parquet(
            result.ambiguous_state_df,
            quarantine_staging / 'ambiguous_state',
        )

        output_staging.rename(
            output_run
        )

        quarantine_staging.rename(
            quarantine_run
        )

        _write_current_pointer_local(
            output_root,
            run_id,
        )

    except Exception:
        _remove_local_if_exists(
            output_staging
        )

        _remove_local_if_exists(
            quarantine_staging
        )

        current_run_id = (
            get_current_run_id(
                output_root
            )
        )

        if current_run_id != run_id:
            _remove_local_if_exists(
                output_run
            )

            _remove_local_if_exists(
                quarantine_run
            )

        raise


def publish_sales_run(
    result: SalesPipelineResult,
    output_root: str | Path,
    quarantine_root: str | Path,
    run_id: str,
) -> None:
    """
    Persist one successfully reconciled P001 run.

    Local:
        stage -> rename -> CURRENT

    URI/object storage:
        immutable final run prefixes -> CURRENT
    """
    is_remote = _assert_matching_storage_kind(
        output_root,
        quarantine_root,
    )

    if is_remote:
        _publish_remote_sales_run(
            result=result,
            output_root=str(output_root),
            quarantine_root=str(quarantine_root),
            run_id=run_id,
        )

        return

    _publish_local_sales_run(
        result=result,
        output_root=Path(output_root),
        quarantine_root=Path(quarantine_root),
        run_id=run_id,
    )


def _published_dataset_location(
    spark: SparkSession,
    root: str | Path,
    run_id: str,
    *parts: str,
) -> str:
    """
    Return a validated local/URI published dataset location.
    """
    if is_uri_location(str(root)):
        location = join_location(
            str(root),
            'runs',
            run_id,
            *parts,
        )

        if not _remote_exists(
            spark,
            location,
        ):
            raise PublicationError(
                'Published dataset is missing for '
                f'run {run_id}: {location}'
            )

        return location

    path = (
        Path(root)
        / 'runs'
        / run_id
    ).joinpath(*parts)

    if not path.exists():
        raise PublicationError(
            'Published dataset is missing for '
            f'run {run_id}: {path}'
        )

    return str(path)


def load_current_analytical_table(
    spark: SparkSession,
    output_root: str | Path,
    table_name: str,
) -> DataFrame | None:
    """
    Load one analytical table from the currently published run.
    """
    if table_name not in ANALYTICAL_TABLE_NAMES:
        raise PublicationError(
            f'Unknown analytical table: {table_name}'
        )

    run_id = get_current_run_id(
        output_root,
        spark=spark,
    )

    if run_id is None:
        return None

    table_location = _published_dataset_location(
        spark,
        output_root,
        run_id,
        'analytical',
        table_name,
    )

    return spark.read.parquet(
        table_location
    )


def load_current_sales_state(
    spark: SparkSession,
    output_root: str | Path,
    quarantine_root: str | Path,
) -> tuple[DataFrame | None, DataFrame | None]:
    """
    Load the state required for the next incremental run.
    """
    _assert_matching_storage_kind(
        output_root,
        quarantine_root,
    )

    run_id = get_current_run_id(
        output_root,
        spark=spark,
    )

    if run_id is None:
        return None, None

    resolved_state_location = (
        _published_dataset_location(
            spark,
            output_root,
            run_id,
            'resolved_state',
        )
    )

    ambiguous_state_location = (
        _published_dataset_location(
            spark,
            quarantine_root,
            run_id,
            'ambiguous_state',
        )
    )

    current_state_df = spark.read.parquet(
        resolved_state_location
    )

    ambiguous_state_df = spark.read.parquet(
        ambiguous_state_location
    )

    return (
        current_state_df,
        ambiguous_state_df,
    )


def load_current_curated_sales(
    spark: SparkSession,
    output_root: str | Path,
) -> DataFrame | None:
    """
    Load the currently published analytical sales dataset.
    """
    run_id = get_current_run_id(
        output_root,
        spark=spark,
    )

    if run_id is None:
        return None

    curated_location = (
        _published_dataset_location(
            spark,
            output_root,
            run_id,
            'curated_sales',
        )
    )

    return spark.read.parquet(
        curated_location
    )
