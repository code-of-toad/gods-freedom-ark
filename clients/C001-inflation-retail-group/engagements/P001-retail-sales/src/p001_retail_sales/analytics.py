"""
SQL analytics support for the P001 star schema.

The SQL itself lives in:

    sql/analytics.sql

This module only:
- loads named SQL queries;
- registers analytical DataFrames as Spark SQL views; and
- executes a selected query.
"""

from pathlib import Path
import re

from pyspark.sql import DataFrame, SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_ANALYTICS_SQL_PATH = (
    PROJECT_ROOT
    / 'sql'
    / 'analytics.sql'
)


QUERY_HEADER_PATTERN = re.compile(
    r'^--\s*name:\s*([a-z][a-z0-9_]*)\s*$',
    re.MULTILINE,
)


class AnalyticsQueryError(RuntimeError):
    """
    Raised when an analytical SQL query cannot be loaded or selected.
    """


def load_analytics_queries(
    sql_path: str | Path | None = None,
) -> dict[str, str]:
    """
    Load all named queries from analytics.sql.

    Query format:

        -- name: daily_sales
        SELECT ...
        ;

        -- name: store_performance
        SELECT ...
        ;
    """
    if sql_path is None:
        sql_path = DEFAULT_ANALYTICS_SQL_PATH

    sql_path = Path(sql_path)

    if not sql_path.exists():
        raise AnalyticsQueryError(
            f'Analytics SQL file does not exist: {sql_path}'
        )

    sql_text = sql_path.read_text(
        encoding='utf-8'
    )

    matches = list(
        QUERY_HEADER_PATTERN.finditer(
            sql_text
        )
    )

    if not matches:
        raise AnalyticsQueryError(
            f'No named analytical queries found: {sql_path}'
        )

    queries = {}

    for index, match in enumerate(matches):
        query_name = match.group(1)

        if query_name in queries:
            raise AnalyticsQueryError(
                f'Duplicate analytical query name: {query_name}'
            )

        query_start = match.end()

        if index + 1 < len(matches):
            query_end = matches[index + 1].start()
        else:
            query_end = len(sql_text)

        query = sql_text[
            query_start:query_end
        ].strip()

        # Spark SQL executes the query itself; the trailing file-level
        # statement delimiter is unnecessary.
        if query.endswith(';'):
            query = query[:-1].rstrip()

        if not query:
            raise AnalyticsQueryError(
                f'Analytical query is empty: {query_name}'
            )

        queries[query_name] = query

    return queries


def register_analytical_views(
    fact_sales_df: DataFrame,
    dim_product_df: DataFrame,
    dim_store_df: DataFrame,
    dim_date_df: DataFrame,
) -> None:
    """
    Register the P001 analytical model as Spark SQL temporary views.

    The view names deliberately match the eventual warehouse table
    names so the analytical SQL remains easy to migrate.
    """
    fact_sales_df.createOrReplaceTempView(
        'fact_sales'
    )

    dim_product_df.createOrReplaceTempView(
        'dim_product'
    )

    dim_store_df.createOrReplaceTempView(
        'dim_store'
    )

    dim_date_df.createOrReplaceTempView(
        'dim_date'
    )


def run_analytics_query(
    spark: SparkSession,
    query_name: str,
    sql_path: str | Path | None = None,
) -> DataFrame:
    """
    Execute one named P001 analytical SQL query.

    Analytical views must already be registered in the active
    Spark session.
    """
    queries = load_analytics_queries(
        sql_path
    )

    query = queries.get(
        query_name
    )

    if query is None:
        available = ', '.join(
            sorted(queries)
        )

        raise AnalyticsQueryError(
            f'Unknown analytical query: {query_name}. '
            f'Available queries: {available}'
        )

    return spark.sql(
        query
    )
