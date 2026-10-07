"""
Distributed cloud benchmark-data generation for P001.

This module intentionally uses Spark expressions rather than Python row loops
so generation can execute across Spark partitions on Managed Spark.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CloudBenchmarkSpec:
    """
    Defines one deterministic distributed sales benchmark dataset.
    """
    row_count: int
    partitions: int = 8
    product_count: int = 1_000
    store_count: int = 100
    sale_day_count: int = 30
    return_every: int = 20


def build_sales_dataframe(
    spark: SparkSession,
    spec: CloudBenchmarkSpec,
) -> DataFrame:
    """
    Build deterministic sales rows as a distributed Spark DataFrame.

    WHAT: derive every field from Spark's distributed range ID.
    WHY: avoid driver-side Python row generation and keep output reproducible.
    """
    if spec.row_count <= 0:
        raise ValueError('row_count must be positive.')

    if spec.partitions <= 0:
        raise ValueError('partitions must be positive.')

    if spec.product_count <= 0:
        raise ValueError('product_count must be positive.')

    if spec.store_count <= 0:
        raise ValueError('store_count must be positive.')

    if spec.sale_day_count <= 0:
        raise ValueError('sale_day_count must be positive.')

    if spec.return_every <= 0:
        raise ValueError('return_every must be positive.')

    source = spark.range(
        start=0,
        end=spec.row_count,
        step=1,
        numPartitions=spec.partitions,
    )

    index = F.col('id')
    row_number = index + F.lit(1)

    product_number = (
        F.pmod(index, F.lit(spec.product_count))
        + F.lit(1)
    )

    store_number = (
        F.pmod(index, F.lit(spec.store_count))
        + F.lit(1)
    )

    sale_day_offset = F.pmod(
        index,
        F.lit(spec.sale_day_count),
    ).cast('int')

    sale_date = F.date_add(
        F.lit('2026-01-01').cast('date'),
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
        F.when(is_return, F.lit(-1))
        .otherwise(
            F.pmod(index, F.lit(5))
            + F.lit(1)
        )
    )

    unit_price = (
        F.lit(10)
        + (
            F.pmod(index, F.lit(100))
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
            F.pmod(row_number, F.lit(10))
            == F.lit(0),
            F.lit(1.00),
        )
        .otherwise(F.lit(0.00))
    ).cast('decimal(12,2)')

    updated_at = (
        F.from_unixtime(
            F.unix_timestamp(
                sale_date.cast('timestamp')
            )
            + F.pmod(index, F.lit(86_400))
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
        .otherwise(F.lit('COMPLETED'))
        .alias('order_status'),
        F.date_format(
            updated_at,
            "yyyy-MM-dd'T'HH:mm:ss",
        ).alias('updated_at'),
    )


def write_sales_csv(
    df: DataFrame,
    output_uri: str,
    *,
    overwrite: bool = False,
) -> None:
    """
    Write the distributed sales DataFrame as uncompressed CSV part files.
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
    path = spark._jvm.org.apache.hadoop.fs.Path(output_uri)
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
            'Generate deterministic distributed P001 sales benchmark data.'
        )
    )

    parser.add_argument(
        '--rows',
        type=int,
        required=True,
        help='Number of sales rows to generate.',
    )

    parser.add_argument(
        '--output',
        required=True,
        help='Output URI, e.g. gs://bucket/benchmark/calibration/sales-1m.',
    )

    parser.add_argument(
        '--partitions',
        type=int,
        default=8,
        help='Number of Spark range/output partitions. Default: 8.',
    )

    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Replace an existing output location.',
    )

    return parser.parse_args(argv)


def main(
    argv: list[str] | None = None,
) -> None:
    args = _parse_args(argv)

    spec = CloudBenchmarkSpec(
        row_count=args.rows,
        partitions=args.partitions,
    )

    spark = (
        SparkSession.builder
        .appName('p001-cloud-benchmark-generator')
        .getOrCreate()
    )

    try:
        sales_df = build_sales_dataframe(
            spark=spark,
            spec=spec,
        )

        write_sales_csv(
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
        print('P001 cloud benchmark generation completed.')
        print(f'Rows: {spec.row_count:,}')
        print(f'Partitions: {spec.partitions}')
        print(f'CSV bytes: {data_bytes:,}')
        print(f'Bytes/row: {bytes_per_row:.2f}')
        print(f'Estimated rows for ~1 GB: {rows_per_gb:,}')
        print(f'Estimated rows for ~5 GB: {rows_per_gb * 5:,}')
        print(f'Estimated rows for ~10 GB: {rows_per_gb * 10:,}')
        print(f'Output: {args.output}')

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
