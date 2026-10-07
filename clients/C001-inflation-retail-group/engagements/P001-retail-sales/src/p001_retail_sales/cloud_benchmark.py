"""
Distributed cloud benchmark-data generation for P001.

Sales generation uses Spark expressions rather than Python row loops so the
large benchmark can execute across Managed Spark workers.

The same module can also generate the small deterministic product/store
reference datasets required by the benchmark sales data.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CloudBenchmarkSpec:
    """
    Defines one deterministic distributed P001 benchmark dataset.
    """

    row_count: int = 0
    partitions: int = 8
    row_offset: int = 0
    base_date: date = date(2026, 1, 1)
    product_count: int = 1_000
    store_count: int = 100
    sale_day_count: int = 30
    return_every: int = 20


def _validate_common_spec(
    spec: CloudBenchmarkSpec,
) -> None:
    if spec.partitions <= 0:
        raise ValueError('partitions must be positive.')

    if spec.row_offset < 0:
        raise ValueError('row_offset must be nonnegative.')

    if spec.product_count <= 0:
        raise ValueError('product_count must be positive.')

    if spec.store_count <= 0:
        raise ValueError('store_count must be positive.')

    if spec.sale_day_count <= 0:
        raise ValueError('sale_day_count must be positive.')

    if spec.return_every <= 0:
        raise ValueError('return_every must be positive.')


def build_sales_dataframe(
    spark: SparkSession,
    spec: CloudBenchmarkSpec,
) -> DataFrame:
    """
    Build deterministic sales rows as a distributed Spark DataFrame.

    row_offset lets separately generated daily batches use non-overlapping
    order IDs while preserving deterministic generation.
    """
    _validate_common_spec(spec)

    if spec.row_count <= 0:
        raise ValueError('row_count must be positive.')

    source = spark.range(
        start=0,
        end=spec.row_count,
        step=1,
        numPartitions=spec.partitions,
    )

    local_index = F.col('id')
    global_index = (
        local_index
        + F.lit(spec.row_offset)
    )

    row_number = (
        global_index
        + F.lit(1)
    )

    product_number = (
        F.pmod(
            global_index,
            F.lit(spec.product_count),
        )
        + F.lit(1)
    )

    store_number = (
        F.pmod(
            global_index,
            F.lit(spec.store_count),
        )
        + F.lit(1)
    )

    sale_day_offset = F.pmod(
        local_index,
        F.lit(spec.sale_day_count),
    ).cast('int')

    sale_date = F.date_add(
        F.lit(
            spec.base_date.isoformat()
        ).cast('date'),
        sale_day_offset,
    )

    is_return = (
        F.pmod(
            row_number,
            F.lit(spec.return_every),
        )
        == F.lit(0)
    )

    quantity = (
        F.when(
            is_return,
            F.lit(-1),
        )
        .otherwise(
            F.pmod(
                global_index,
                F.lit(5),
            )
            + F.lit(1)
        )
    )

    unit_price = (
        F.lit(10)
        + (
            F.pmod(
                global_index,
                F.lit(100),
            )
            / F.lit(100)
        )
    ).cast('decimal(12,2)')

    unit_cost = (
        unit_price
        * F.lit(0.60)
    ).cast('decimal(12,2)')

    discount_amount = (
        F.when(
            is_return,
            F.lit(0.00),
        )
        .when(
            F.pmod(
                row_number,
                F.lit(10),
            )
            == F.lit(0),
            F.lit(1.00),
        )
        .otherwise(
            F.lit(0.00)
        )
    ).cast('decimal(12,2)')

    updated_at = (
        F.from_unixtime(
            F.unix_timestamp(
                sale_date.cast('timestamp')
            )
            + F.pmod(
                global_index,
                F.lit(86_400),
            )
        )
        .cast('timestamp')
    )

    return source.select(
        F.concat(
            F.lit('O'),
            F.lpad(
                row_number.cast('string'),
                12,
                '0',
            ),
        ).alias('order_id'),
        F.lit(1).alias('line_id'),
        sale_date.cast('string').alias('sale_date'),
        F.concat(
            F.lit('S'),
            F.lpad(
                store_number.cast('string'),
                4,
                '0',
            ),
        ).alias('store_id'),
        F.concat(
            F.lit('P'),
            F.lpad(
                product_number.cast('string'),
                6,
                '0',
            ),
        ).alias('product_id'),
        quantity.alias('quantity'),
        unit_price.alias('unit_price'),
        unit_cost.alias('unit_cost'),
        discount_amount.alias('discount_amount'),
        F.when(
            is_return,
            F.lit('RETURNED'),
        )
        .otherwise(
            F.lit('COMPLETED')
        )
        .alias('order_status'),
        F.date_format(
            updated_at,
            "yyyy-MM-dd'T'HH:mm:ss",
        ).alias('updated_at'),
    )


def build_products_dataframe(
    spark: SparkSession,
    spec: CloudBenchmarkSpec,
) -> DataFrame:
    """
    Build the complete deterministic benchmark product reference dataset.
    """
    _validate_common_spec(spec)

    source = spark.range(
        start=0,
        end=spec.product_count,
        step=1,
        numPartitions=1,
    )

    product_number = (
        F.col('id')
        + F.lit(1)
    )

    category_index = F.pmod(
        F.col('id'),
        F.lit(4),
    )

    return source.select(
        F.concat(
            F.lit('P'),
            F.lpad(
                product_number.cast('string'),
                6,
                '0',
            ),
        ).alias('product_id'),
        F.concat(
            F.lit('Product '),
            product_number.cast('string'),
        ).alias('product_name'),
        (
            F.when(
                category_index == 0,
                F.lit('Grocery'),
            )
            .when(
                category_index == 1,
                F.lit('Electronics'),
            )
            .when(
                category_index == 2,
                F.lit('Home'),
            )
            .otherwise(
                F.lit('Apparel')
            )
        ).alias('category'),
        F.lit('true').alias('active'),
    )


def build_stores_dataframe(
    spark: SparkSession,
    spec: CloudBenchmarkSpec,
) -> DataFrame:
    """
    Build the complete deterministic benchmark store reference dataset.
    """
    _validate_common_spec(spec)

    source = spark.range(
        start=0,
        end=spec.store_count,
        step=1,
        numPartitions=1,
    )

    store_number = (
        F.col('id')
        + F.lit(1)
    )

    province_index = F.pmod(
        F.col('id'),
        F.lit(4),
    )

    return source.select(
        F.concat(
            F.lit('S'),
            F.lpad(
                store_number.cast('string'),
                4,
                '0',
            ),
        ).alias('store_id'),
        F.concat(
            F.lit('Store '),
            store_number.cast('string'),
        ).alias('store_name'),
        F.concat(
            F.lit('City '),
            store_number.cast('string'),
        ).alias('city'),
        (
            F.when(
                province_index == 0,
                F.lit('ON'),
            )
            .when(
                province_index == 1,
                F.lit('BC'),
            )
            .when(
                province_index == 2,
                F.lit('AB'),
            )
            .otherwise(
                F.lit('QC')
            )
        ).alias('province'),
        F.lit('true').alias('active'),
    )


def write_csv(
    df: DataFrame,
    output_uri: str,
    *,
    overwrite: bool = False,
) -> None:
    """
    Write a DataFrame as uncompressed CSV part files.
    """
    mode = (
        'overwrite'
        if overwrite
        else 'errorifexists'
    )

    (
        df.write
        .mode(mode)
        .option('header', True)
        .csv(output_uri)
    )


def _sum_part_file_bytes(
    spark: SparkSession,
    output_uri: str,
) -> int:
    """
    Sum only generated CSV part-file bytes through Hadoop's filesystem API.
    """
    path = spark._jvm.org.apache.hadoop.fs.Path(
        output_uri
    )

    filesystem = path.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )

    total_bytes = 0

    for status in filesystem.listStatus(path):
        name = status.getPath().getName()

        if (
            status.isFile()
            and name.startswith('part-')
        ):
            total_bytes += status.getLen()

    return total_bytes


def _parse_args(
    argv: list[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            'Generate deterministic distributed P001 benchmark data.'
        )
    )

    parser.add_argument(
        '--dataset',
        choices=[
            'sales',
            'references',
        ],
        default='sales',
        help=(
            'Dataset mode. "sales" generates one sales batch; '
            '"references" generates products.csv/ and stores.csv/. '
            'Default: sales.'
        ),
    )

    parser.add_argument(
        '--rows',
        type=int,
        help='Number of sales rows to generate. Required for --dataset sales.',
    )

    parser.add_argument(
        '--output',
        required=True,
        help=(
            'For sales: exact output URI. '
            'For references: benchmark input root URI.'
        ),
    )

    parser.add_argument(
        '--partitions',
        type=int,
        default=8,
        help='Number of Spark sales/output partitions. Default: 8.',
    )

    parser.add_argument(
        '--row-offset',
        type=int,
        default=0,
        help=(
            'Starting logical row offset used to keep independently '
            'generated sales batches non-overlapping. Default: 0.'
        ),
    )

    parser.add_argument(
        '--base-date',
        type=date.fromisoformat,
        default=date(2026, 1, 1),
        help='First sales date in YYYY-MM-DD form. Default: 2026-01-01.',
    )

    parser.add_argument(
        '--product-count',
        type=int,
        default=1_000,
        help='Number of benchmark products. Default: 1000.',
    )

    parser.add_argument(
        '--store-count',
        type=int,
        default=100,
        help='Number of benchmark stores. Default: 100.',
    )

    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Replace existing output locations.',
    )

    args = parser.parse_args(argv)

    if (
        args.dataset == 'sales'
        and args.rows is None
    ):
        parser.error(
            '--rows is required when --dataset sales.'
        )

    return args


def _run_sales_generation(
    spark: SparkSession,
    args: argparse.Namespace,
) -> None:
    spec = CloudBenchmarkSpec(
        row_count=args.rows,
        partitions=args.partitions,
        row_offset=args.row_offset,
        base_date=args.base_date,
        product_count=args.product_count,
        store_count=args.store_count,
    )

    sales_df = build_sales_dataframe(
        spark=spark,
        spec=spec,
    )

    write_csv(
        df=sales_df,
        output_uri=args.output,
        overwrite=args.overwrite,
    )

    data_bytes = _sum_part_file_bytes(
        spark=spark,
        output_uri=args.output,
    )

    bytes_per_row = (
        data_bytes
        / spec.row_count
    )

    rows_per_gb = int(
        1_000_000_000
        / bytes_per_row
    )

    print()
    print(
        'P001 cloud benchmark generation completed.'
    )
    print(f'Rows: {spec.row_count:,}')
    print(f'Partitions: {spec.partitions}')
    print(f'Row offset: {spec.row_offset:,}')
    print(
        'Base date: '
        f'{spec.base_date.isoformat()}'
    )
    print(f'CSV bytes: {data_bytes:,}')
    print(
        f'Bytes/row: {bytes_per_row:.2f}'
    )
    print(
        'Estimated rows for ~1 GB: '
        f'{rows_per_gb:,}'
    )
    print(
        'Estimated rows for ~5 GB: '
        f'{rows_per_gb * 5:,}'
    )
    print(
        'Estimated rows for ~10 GB: '
        f'{rows_per_gb * 10:,}'
    )
    print(f'Output: {args.output}')


def _run_reference_generation(
    spark: SparkSession,
    args: argparse.Namespace,
) -> None:
    spec = CloudBenchmarkSpec(
        product_count=args.product_count,
        store_count=args.store_count,
    )

    root = args.output.rstrip('/')

    products_uri = (
        f'{root}/products.csv'
    )

    stores_uri = (
        f'{root}/stores.csv'
    )

    products_df = build_products_dataframe(
        spark=spark,
        spec=spec,
    )

    stores_df = build_stores_dataframe(
        spark=spark,
        spec=spec,
    )

    write_csv(
        df=products_df,
        output_uri=products_uri,
        overwrite=args.overwrite,
    )

    write_csv(
        df=stores_df,
        output_uri=stores_uri,
        overwrite=args.overwrite,
    )

    print()
    print(
        'P001 cloud benchmark reference generation completed.'
    )
    print(
        f'Products: {spec.product_count:,}'
    )
    print(
        f'Stores: {spec.store_count:,}'
    )
    print(
        f'Products output: {products_uri}'
    )
    print(
        f'Stores output: {stores_uri}'
    )


def main(
    argv: list[str] | None = None,
) -> None:
    args = _parse_args(argv)

    spark = (
        SparkSession.builder
        .appName(
            'p001-cloud-benchmark-generator'
        )
        .getOrCreate()
    )

    try:
        if args.dataset == 'references':
            _run_reference_generation(
                spark=spark,
                args=args,
            )
        else:
            _run_sales_generation(
                spark=spark,
                args=args,
            )

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
