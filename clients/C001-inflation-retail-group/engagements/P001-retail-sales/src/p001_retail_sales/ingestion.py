"""
Raw data ingestion for P001.
"""
from pyspark.sql import DataFrame, SparkSession
from p001_retail_sales.schemas import (
    RAW_SALES_SCHEMA,
    RAW_PRODUCTS_SCHEMA,
    RAW_STORES_SCHEMA,
)


def read_raw_sales(spark: SparkSession, path: str) -> DataFrame:
    """
    Read raw sales CSV w/o applying business validation.
    """
    return (
        spark.read
        .option('header', True)
        .schema(RAW_SALES_SCHEMA)
        .csv(path)
    )

def read_raw_products(spark: SparkSession, path: str) -> DataFrame:
    """
    Read raw products CSV w/o applying business validation.
    """
    return (
        spark.read
        .option('header', True)
        .schema(RAW_PRODUCTS_SCHEMA)
        .csv(path)
    )

def read_raw_stores(spark: SparkSession, path: str) -> DataFrame:
    """
    Read raw stores CSV w/o applying business validation.
    """
    return (
        spark.read
        .option('header', True)
        .schema(RAW_STORES_SCHEMA)
        .csv(path)
    )
