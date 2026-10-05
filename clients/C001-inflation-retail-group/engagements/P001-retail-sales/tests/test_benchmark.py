"""
Tests for P001 benchmark-data generation.
"""

import csv

from p001_retail_sales.benchmark import (
    BenchmarkSpec,
    generate_benchmark_inputs,
)


def test_generate_benchmark_inputs(
    tmp_path,
):
    spec = BenchmarkSpec(
        row_count=100,
        product_count=10,
        store_count=5,
        sale_day_count=3,
        return_every=20,
    )

    sales_path, products_path, stores_path = (
        generate_benchmark_inputs(
            tmp_path,
            spec,
        )
    )

    assert sales_path.exists()
    assert products_path.exists()
    assert stores_path.exists()


def test_benchmark_generator_produces_expected_counts(
    tmp_path,
):
    spec = BenchmarkSpec(
        row_count=100,
        product_count=10,
        store_count=5,
    )

    sales_path, products_path, stores_path = (
        generate_benchmark_inputs(
            tmp_path,
            spec,
        )
    )

    with sales_path.open(
        encoding='utf-8',
        newline='',
    ) as file:
        sales_rows = list(
            csv.DictReader(file)
        )

    with products_path.open(
        encoding='utf-8',
        newline='',
    ) as file:
        product_rows = list(
            csv.DictReader(file)
        )

    with stores_path.open(
        encoding='utf-8',
        newline='',
    ) as file:
        store_rows = list(
            csv.DictReader(file)
        )

    assert len(sales_rows) == 100
    assert len(product_rows) == 10
    assert len(store_rows) == 5


def test_benchmark_generator_creates_valid_returns(
    tmp_path,
):
    spec = BenchmarkSpec(
        row_count=40,
        product_count=10,
        store_count=5,
        return_every=20,
    )

    sales_path, _, _ = (
        generate_benchmark_inputs(
            tmp_path,
            spec,
        )
    )

    with sales_path.open(
        encoding='utf-8',
        newline='',
    ) as file:
        rows = list(
            csv.DictReader(file)
        )

    returns = [
        row
        for row in rows
        if row['order_status'] == 'RETURNED'
    ]

    assert len(returns) == 2

    assert all(
        row['quantity'] == '-1'
        for row in returns
    )


def test_benchmark_generation_is_deterministic(
    tmp_path,
):
    spec = BenchmarkSpec(
        row_count=25,
        product_count=5,
        store_count=3,
    )

    first_root = (
        tmp_path
        / 'first'
    )

    second_root = (
        tmp_path
        / 'second'
    )

    first_sales, _, _ = (
        generate_benchmark_inputs(
            first_root,
            spec,
        )
    )

    second_sales, _, _ = (
        generate_benchmark_inputs(
            second_root,
            spec,
        )
    )

    assert (
        first_sales.read_text(
            encoding='utf-8'
        )
        ==
        second_sales.read_text(
            encoding='utf-8'
        )
    )
