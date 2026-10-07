"""
Tests for distributed cloud benchmark generation.
"""
from datetime import date

from p001_retail_sales.cloud_benchmark import (
    CloudBenchmarkSpec,
    build_products_dataframe,
    build_sales_dataframe,
    build_stores_dataframe,
)


def test_cloud_benchmark_generator_produces_expected_rows(
    spark,
):
    spec = CloudBenchmarkSpec(
        row_count=40,
        partitions=2,
        product_count=10,
        store_count=5,
        sale_day_count=3,
        return_every=20,
    )

    df = build_sales_dataframe(
        spark=spark,
        spec=spec,
    )

    assert df.count() == 40

    rows = {
        row['order_id']: row
        for row in (
            df
            .where(
                df.order_id.isin(
                    'O000000000001',
                    'O000000000020',
                )
            )
            .collect()
        )
    }

    first = rows['O000000000001']

    assert first['line_id'] == 1
    assert first['sale_date'] == '2026-01-01'
    assert first['store_id'] == 'S0001'
    assert first['product_id'] == 'P000001'
    assert first['quantity'] == 1
    assert first['order_status'] == 'COMPLETED'
    assert first['updated_at'] == '2026-01-01T00:00:00'

    twentieth = rows['O000000000020']

    assert twentieth['quantity'] == -1
    assert twentieth['order_status'] == 'RETURNED'
    assert str(
        twentieth['discount_amount']
    ) == '0.00'


def test_cloud_benchmark_generation_is_deterministic(
    spark,
):
    spec = CloudBenchmarkSpec(
        row_count=100,
        partitions=2,
        product_count=10,
        store_count=5,
    )

    first_df = build_sales_dataframe(
        spark=spark,
        spec=spec,
    )

    second_df = build_sales_dataframe(
        spark=spark,
        spec=spec,
    )

    assert (
        first_df.exceptAll(
            second_df
        ).count()
        == 0
    )

    assert (
        second_df.exceptAll(
            first_df
        ).count()
        == 0
    )


def test_cloud_benchmark_supports_nonoverlapping_batches(
    spark,
):
    day_one = build_sales_dataframe(
        spark=spark,
        spec=CloudBenchmarkSpec(
            row_count=2,
            partitions=1,
            row_offset=0,
            base_date=date(2026, 1, 1),
        ),
    ).orderBy('order_id').collect()

    day_two = build_sales_dataframe(
        spark=spark,
        spec=CloudBenchmarkSpec(
            row_count=2,
            partitions=1,
            row_offset=2,
            base_date=date(2026, 2, 1),
        ),
    ).orderBy('order_id').collect()

    assert (
        day_one[0]['order_id']
        == 'O000000000001'
    )

    assert (
        day_two[0]['order_id']
        == 'O000000000003'
    )

    assert (
        day_one[0]['sale_date']
        == '2026-01-01'
    )

    assert (
        day_two[0]['sale_date']
        == '2026-02-01'
    )


def test_cloud_benchmark_reference_data_covers_sales_keys(
    spark,
):
    spec = CloudBenchmarkSpec(
        row_count=1_000,
        partitions=2,
        product_count=10,
        store_count=5,
    )

    sales_df = build_sales_dataframe(
        spark=spark,
        spec=spec,
    )

    products_df = build_products_dataframe(
        spark=spark,
        spec=spec,
    )

    stores_df = build_stores_dataframe(
        spark=spark,
        spec=spec,
    )

    assert products_df.count() == 10
    assert stores_df.count() == 5

    missing_products = (
        sales_df
        .select('product_id')
        .distinct()
        .join(
            products_df.select('product_id'),
            on='product_id',
            how='left_anti',
        )
        .count()
    )

    missing_stores = (
        sales_df
        .select('store_id')
        .distinct()
        .join(
            stores_df.select('store_id'),
            on='store_id',
            how='left_anti',
        )
        .count()
    )

    assert missing_products == 0
    assert missing_stores == 0


def test_cloud_benchmark_reference_rows_are_valid_shape(
    spark,
):
    spec = CloudBenchmarkSpec(
        product_count=4,
        store_count=4,
    )

    products = (
        build_products_dataframe(
            spark=spark,
            spec=spec,
        )
        .orderBy('product_id')
        .collect()
    )

    stores = (
        build_stores_dataframe(
            spark=spark,
            spec=spec,
        )
        .orderBy('store_id')
        .collect()
    )

    assert products[0]['product_id'] == 'P000001'
    assert products[0]['category'] == 'Grocery'
    assert products[0]['active'] == 'true'

    assert stores[0]['store_id'] == 'S0001'
    assert stores[0]['province'] == 'ON'
    assert stores[0]['active'] == 'true'
