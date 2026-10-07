"""
Command-line entry point for the P001 retail-sales batch job.
"""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from p001_retail_sales.config import (
    is_uri_location,
    join_location,
    load_config,
    require_location,
)
from p001_retail_sales.job import run_sales_batch_job
from p001_retail_sales.publication import get_current_run_id


def _parse_args(
    argv: list[str] | None = None,
) -> argparse.Namespace:
    """
    Parse P001 batch-job command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            'Run one incremental P001 retail-sales batch.'
        )
    )

    parser.add_argument(
        '--environment',
        '--env',
        dest='environment',
        choices=[
            'dev',
            'test',
            'prod',
        ],
        default='dev',
        help='Runtime environment. Default: dev.',
    )

    parser.add_argument(
        '--config-dir',
        default=None,
        help=(
            'Optional local directory containing base.yaml and '
            '<environment>.yaml. Useful when Managed Spark localizes '
            'configuration files into the driver working directory.'
        ),
    )

    parser.add_argument(
        '--sales-file',
        required=True,
        help=(
            'Sales delivery name inside the configured input/sales location. '
            'This may identify either one CSV file or a Spark-readable '
            'directory of CSV part files.'
        ),
    )

    parser.add_argument(
        '--run-id',
        required=True,
        help=(
            'Unique identifier for this physical pipeline run.'
        ),
    )

    return parser.parse_args(argv)


def _create_spark(
    config: dict,
) -> SparkSession:
    """
    Create the Spark session described by runtime configuration.
    """
    spark_config = config.get(
        'spark',
        {},
    )

    app_name = spark_config.get(
        'app_name',
        'p001-retail-sales',
    )

    builder = (
        SparkSession.builder
        .appName(app_name)
    )

    master = spark_config.get(
        'master'
    )

    if master:
        builder = builder.master(
            master
        )

    return builder.getOrCreate()


def _require_input_file(
    location: str,
) -> None:
    """
    Fail early when an expected LOCAL input file does not exist.

    URI-based inputs are validated by Spark/the remote storage connector
    when Spark attempts to read them.
    """
    if is_uri_location(location):
        return

    path = Path(location)

    if not path.is_file():
        raise FileNotFoundError(
            f'Input file does not exist: {path}'
        )


def main(
    argv: list[str] | None = None,
) -> None:
    """
    Execute one configured P001 batch run.
    """
    args = _parse_args(argv)

    config = load_config(
        args.environment,
        config_dir=args.config_dir,
    )

    input_root = require_location(
        config,
        'input',
    )

    output_root = require_location(
        config,
        'output',
    )

    quarantine_root = require_location(
        config,
        'quarantine',
    )

    sales_path = join_location(
        input_root,
        'sales',
        args.sales_file,
    )

    products_path = join_location(
        input_root,
        'products.csv',
    )

    stores_path = join_location(
        input_root,
        'stores.csv',
    )

    _require_input_file(
        sales_path
    )

    _require_input_file(
        products_path
    )

    _require_input_file(
        stores_path
    )

    spark = _create_spark(
        config
    )

    try:
        run_sales_batch_job(
            spark=spark,
            sales_path=sales_path,
            products_path=products_path,
            stores_path=stores_path,
            output_root=output_root,
            quarantine_root=quarantine_root,
            run_id=args.run_id,
        )

        current_run_id = get_current_run_id(
            output_root,
            spark=spark,
        )

        print()
        print(
            'P001 batch completed successfully.'
        )
        print(
            f'environment: {args.environment}'
        )
        print(
            f'Run ID: {args.run_id}'
        )
        print(
            f'CURRENT: {current_run_id}'
        )
        print(
            f'Output root: {output_root}'
        )
        print(
            f'Quarantine root: {quarantine_root}'
        )

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
