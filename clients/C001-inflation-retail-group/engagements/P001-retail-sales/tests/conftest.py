"""
Shared pytest fixtures for P001.
"""
import pytest
from collections.abc import Generator
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark() -> Generator[SparkSession, None, None]:
    """
    Provide one local SparkSession for the test suite.
    """
    session = (
        SparkSession.builder
        .appName('p001-retail-sales-tests')
        .master('local[2]')
        .config('spark.ui.enabled', 'false')
        .config('spark.sql.shuffle.partitions', '2')
        .getOrCreate()
    )
    yield session

    session.stop()
