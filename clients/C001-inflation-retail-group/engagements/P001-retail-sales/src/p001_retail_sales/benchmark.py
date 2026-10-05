"""
Deterministic local benchmark support for P001.

This module exists to measure the current pipeline before optimization.

Benchmark data is generated under data/benchmark/, which is ignored by Git.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession

from p001_retail_sales.job import run_sales_batch_job


@dataclass(frozen=True)
class BenchmarkSpec:
    """
    Defines one deterministic local benchmark dataset.
    """

    row_count: int

    product_count: int = 1_000

    store_count: int = 100

    sale_day_count: int = 30

    return_every: int = 20


@dataclass(frozen=True)
class BenchmarkResult:
    """
    Measurements produced by one benchmark execution.
    """

    row_count: int

    input_bytes: int

    elapsed_seconds: float

    rows_per_second: float

    megabytes_per_second: float

    spark_master: str

    default_parallelism: int

    shuffle_partitions: int

    adaptive_execution_enabled: bool


def generate_benchmark_inputs(
    root: str | Path,
    spec: BenchmarkSpec,
) -> tuple[Path, Path, Path]:
    """
    Generate deterministic product, store, and sales CSV inputs.

    Returns:
        sales_path,
        products_path,
        stores_path
    """
    root = Path(root)

    input_root = root / 'input'
    sales_root = input_root / 'sales'

    input_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    sales_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    products_path = (
        input_root
        / 'products.csv'
    )

    stores_path = (
        input_root
        / 'stores.csv'
    )

    sales_path = (
        sales_root
        / 'sales.csv'
    )

    _write_products(
        products_path,
        spec,
    )

    _write_stores(
        stores_path,
        spec,
    )

    _write_sales(
        sales_path,
        spec,
    )

    return (
        sales_path,
        products_path,
        stores_path,
    )


def _write_products(
    path: Path,
    spec: BenchmarkSpec,
) -> None:
    """
    Generate canonical product reference data.
    """
    categories = [
        'Grocery',
        'Electronics',
        'Home',
        'Apparel',
    ]

    with path.open(
        'w',
        encoding='utf-8',
        newline='',
    ) as file:
        writer = csv.writer(file)

        writer.writerow([
            'product_id',
            'product_name',
            'category',
            'active',
        ])

        for index in range(
            spec.product_count
        ):
            product_number = index + 1

            writer.writerow([
                f'P{product_number:06d}',
                f'Product {product_number}',
                categories[
                    index
                    % len(categories)
                ],
                'true',
            ])


def _write_stores(
    path: Path,
    spec: BenchmarkSpec,
) -> None:
    """
    Generate canonical store reference data.
    """
    provinces = [
        'ON',
        'BC',
        'AB',
        'QC',
    ]

    with path.open(
        'w',
        encoding='utf-8',
        newline='',
    ) as file:
        writer = csv.writer(file)

        writer.writerow([
            'store_id',
            'store_name',
            'city',
            'province',
            'active',
        ])

        for index in range(
            spec.store_count
        ):
            store_number = index + 1

            writer.writerow([
                f'S{store_number:04d}',
                f'Store {store_number}',
                f'City {store_number}',
                provinces[
                    index
                    % len(provinces)
                ],
                'true',
            ])


def _write_sales(
    path: Path,
    spec: BenchmarkSpec,
) -> None:
    """
    Generate deterministic valid sales rows.

    The baseline intentionally avoids duplicate, ambiguous, and invalid
    records. Those will become separate benchmark scenarios later.
    """
    base_date = date(
        2026,
        1,
        1,
    )

    with path.open(
        'w',
        encoding='utf-8',
        newline='',
    ) as file:
        writer = csv.writer(file)

        writer.writerow([
            'order_id',
            'line_id',
            'sale_date',
            'store_id',
            'product_id',
            'quantity',
            'unit_price',
            'unit_cost',
            'discount_amount',
            'order_status',
            'updated_at',
        ])

        for index in range(
            spec.row_count
        ):
            row_number = index + 1

            product_number = (
                index
                % spec.product_count
            ) + 1

            store_number = (
                index
                % spec.store_count
            ) + 1

            day_offset = (
                index
                % spec.sale_day_count
            )

            sale_date = (
                base_date
                + timedelta(
                    days=day_offset
                )
            )

            is_return = (
                row_number
                % spec.return_every
                == 0
            )

            if is_return:
                quantity = -1
                order_status = 'RETURNED'
                discount_amount = '0.00'
            else:
                quantity = (
                    index
                    % 5
                ) + 1

                order_status = 'COMPLETED'

                discount_amount = (
                    '1.00'
                    if row_number % 10 == 0
                    else '0.00'
                )

            cents = (
                index
                % 100
            )

            unit_price = (
                10
                + cents / 100
            )

            unit_cost = (
                unit_price
                * 0.60
            )

            updated_at = datetime(
                sale_date.year,
                sale_date.month,
                sale_date.day,
            ) + timedelta(
                seconds=(
                    index
                    % 86_400
                )
            )

            writer.writerow([
                f'O{row_number:012d}',
                1,
                sale_date.isoformat(),
                f'S{store_number:04d}',
                f'P{product_number:06d}',
                quantity,
                f'{unit_price:.2f}',
                f'{unit_cost:.2f}',
                discount_amount,
                order_status,
                updated_at.strftime(
                    '%Y-%m-%dT%H:%M:%S'
                ),
            ])


def run_benchmark(
    spark: SparkSession,
    benchmark_root: str | Path,
    spec: BenchmarkSpec,
    run_id: str = 'baseline',
) -> BenchmarkResult:
    """
    Generate and execute one fresh P001 benchmark run.

    The benchmark uses isolated output/quarantine state so it does not
    interfere with normal dev CURRENT state.
    """
    benchmark_root = Path(
        benchmark_root
    )

    sales_path, products_path, stores_path = (
        generate_benchmark_inputs(
            benchmark_root,
            spec,
        )
    )

    output_root = (
        benchmark_root
        / 'output'
    )

    quarantine_root = (
        benchmark_root
        / 'quarantine'
    )

    start = perf_counter()

    run_sales_batch_job(
        spark=spark,
        sales_path=sales_path,
        products_path=products_path,
        stores_path=stores_path,
        output_root=output_root,
        quarantine_root=quarantine_root,
        run_id=run_id,
    )

    elapsed_seconds = (
        perf_counter()
        - start
    )

    input_bytes = (
        sales_path.stat().st_size
    )

    rows_per_second = (
        spec.row_count
        / elapsed_seconds
    )

    megabytes = (
        input_bytes
        / 1_000_000
    )

    megabytes_per_second = (
        megabytes
        / elapsed_seconds
    )

    return BenchmarkResult(
        row_count=spec.row_count,
        input_bytes=input_bytes,
        elapsed_seconds=elapsed_seconds,
        rows_per_second=rows_per_second,
        megabytes_per_second=megabytes_per_second,
        spark_master=spark.sparkContext.master,
        default_parallelism=(
            spark.sparkContext.defaultParallelism
        ),
        shuffle_partitions=int(
            spark.conf.get(
                'spark.sql.shuffle.partitions'
            )
        ),
        adaptive_execution_enabled=(
            spark.conf.get(
                'spark.sql.adaptive.enabled'
            ).lower()
            == 'true'
        ),
    )
