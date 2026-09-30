"""
Executable schemas for P001 datasets.
"""
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DecimalType,
    BooleanType,
    DateType,
    TimestampType,
    StructType,
    StructField,
)


# =============================================================================
# RAW INGESTION SCHEMAS
# =============================================================================

RAW_PRODUCTS_SCHEMA = StructType([
    StructField('product_id',   StringType(), nullable=True),
    StructField('product_name', StringType(), nullable=True),
    StructField('category',     StringType(), nullable=True),
    StructField('active',       StringType(), nullable=True),
])

RAW_STORES_SCHEMA = StructType([
    StructField('store_id',   StringType(), nullable=True),
    StructField('store_name', StringType(), nullable=True),
    StructField('city',       StringType(), nullable=True),
    StructField('province',   StringType(), nullable=True),
    StructField('active',     StringType(), nullable=True),
])

RAW_SALES_SCHEMA = StructType([
    StructField('order_id',   StringType(), nullable=True),
    StructField('line_id',    StringType(), nullable=True),
    StructField('sale_date',  StringType(), nullable=True),
    StructField('store_id',   StringType(), nullable=True),
    StructField('product_id', StringType(), nullable=True),
    StructField('quantity',   StringType(), nullable=True),
    StructField('unit_price', StringType(), nullable=True),
    StructField('unit_cost',  StringType(), nullable=True),
    StructField('discount_amount', StringType(), nullable=True),
    StructField('order_status',    StringType(), nullable=True),
    StructField('updated_at',      StringType(), nullable=True),
])


# =============================================================================
# CANONICAL SCHEMAS
# =============================================================================

PRODUCTS_SCHEMA = StructType([
    StructField('product_id',   StringType(),  nullable=False),
    StructField('product_name', StringType(),  nullable=False),
    StructField('category',     StringType(),  nullable=False),
    StructField('active',       BooleanType(), nullable=False),
])

STORES_SCHEMA = StructType([
    StructField('store_id',   StringType(),  nullable=False),
    StructField('store_name', StringType(),  nullable=False),
    StructField('city',       StringType(),  nullable=False),
    StructField('province',   StringType(),  nullable=False),
    StructField('active',     BooleanType(), nullable=False),
])

SALES_SCHEMA = StructType([
    StructField('order_id',   StringType(),  nullable=False),
    StructField('line_id',    IntegerType(), nullable=False),
    StructField('sale_date',  DateType(),    nullable=False),
    StructField('store_id',   StringType(),  nullable=False),
    StructField('product_id', StringType(),  nullable=False),
    StructField('quantity',   IntegerType(), nullable=False),
    StructField('unit_price', DecimalType(12, 2), nullable=False),
    StructField('unit_cost',  DecimalType(12, 2), nullable=False),
    StructField('discount_amount', DecimalType(12, 2), nullable=False),
    StructField('order_status',    StringType(),       nullable=False),
    StructField('updated_at',      TimestampType(),    nullable=False),
])
