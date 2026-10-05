"""
Command-line entry point for the P001 retail-sales batch job.
"""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from p001_retail_sales.config import load_config, require_path
from p001_retail_sales.job import run_sales_batch_job
from p001_retail_sales.publication import get_current_run_id


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    Parse P001 batch-job command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Run one incremental P001 retail-sales batch.'
    )

    parser.add_argument(
        '--environment',
        '--env',
        dest='environment',
        choices=['dev', 'test', 'prod'],
        default='dev',
        help='Runtime environment. Default: dev.'
    )

    parser.add_argument(
        '--sales-file',
        required=True,
        help=(
            'Sales delivery filename inside the configured input/sales directory.'
        ),
    )

    parser.add_argument(
        '--run-id',
        required=True,
        help='Unique identigier for this physical pipeline run.'
    )

    return parser.parse_args(argv)


def _create_spark(config: dict) -> SparkSession:
    """
    Create the Spark session described by runtime configuration.
    """
    spark_config = config.get('spark', {})

    app_name = spark_config.get('app_name', 'p001-retail-sales')

    builder = SparkSession.builder.appName(app_name)

    master = spark_config.get('master')

    # Local environments explicitly configure a master.
    # Cloud environments may instead receive one from their execution platform.
    if master:
        builder = builder.master(master)

    return builder.getOrCreate()


def _require_input_file(path: Path) -> None:
    """
    Fail clearly before Spark starts processing when an expected
    local input file does not exist.
    """
    if not path.is_file():
        raise FileNotFoundError(f'Input file does not exist: {path}')


def main(argv: list[str] | None = None) -> None:
    """
    Execute one configured P001 batch run.
    """
    args = _parse_args(argv)
    config = load_config(args.environment)
    input_root = require_path(config, 'input')
    output_root = require_path(config, 'output')
    quarantine_root = require_path(config, 'quarantine')
    sales_path = input_root / 'sales' / args.sales_file
    products_path = input_root / 'products.csv'
    stores_path = input_root / 'stores.csv'

    _require_input_file(sales_path)
    _require_input_file(products_path)
    _require_input_file(stores_path)

    spark = _create_spark(config)

    try:
        result = run_sales_batch_job(
            spark=spark,
            sales_path=sales_path,
            products_path=products_path,
            stores_path=stores_path,
            output_root=output_root,
            quarantine_root=quarantine_root,
            run_id=args.run_id,
        )

        current_run_id = get_current_run_id(output_root)

        print()
        print('P001 batch completed successfully.')
        print(f'environment: {args.environment}')
        print(f'Run ID: {args.run_id}')
        print(f'CURRENT: {current_run_id}')
        print(f'Trusted rows: {result.candidate_df.count()}')
        print(f'Validation quarantine rows: {result.validation_quarantine_df.count()}')
        print(f'Ambiguous current-state rows: {result.ambiguous_state_df.count()}')
        print(f'Product quarantine rows: {result.products_quarantine_df.count()}')
        print(f'Store quarantine rows: {result.stores_quarantine_df.count()}')
        print(f'Output root: {output_root}')
        print(f'Quarantine root: {quarantine_root}')

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
