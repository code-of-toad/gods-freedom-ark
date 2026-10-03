"""
Validation and quarantine rules for P001.

1. Row-level validity
       required fields, types, ranges, status rules

2. Reference-data validity
       products/stores themselves are trustworthy

3. Referential integrity
       sales.product_id -> products.product_id
       sales.store_id   -> stores.store_id
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _reason(condition: Column, code: str) -> Column:
    """
    Return a single-element reason array when a rule fails.
    Otherwise, return an empty string array.
    """
    return (
        F.when(
            condition,
            F.array(F.lit(code))
        ).otherwise(
            F.array().cast('array<string>')
        )
    )


def add_sales_rejection_reasons(df: DataFrame) -> DataFrame:
    """
    Apply row-level sales validation rules.
    """
    gross_sales = (
        F.col('quantity') * F.col('unit_price')
    )

    rejection_reasons = F.concat(
        _reason(
            F.col('order_id').isNull() | (F.col('order_id') == ''),
            'MISSING_ORDER_ID',
        ),

        _reason(
            F.col('line_id').isNull() | (F.col('line_id') <= 0),
            'INVALID_LINE_ID',
        ),

        _reason(
            F.col('sale_date').isNull(),
            'INVALID_SALE_DATE',
        ),

        _reason(
            F.col('store_id').isNull() | (F.col('store_id') == ''),
            'MISSING_STORE_ID',
        ),

        _reason(
            F.col('product_id').isNull() | (F.col('product_id') == ''),
            'MISSING_PRODUCT_ID',
        ),

        _reason(
            F.col('quantity').isNull()
            | (F.col('quantity') == 0)
            | ((F.col('order_status') == 'COMPLETED') & (F.col('quantity') <= 0))
            | ((F.col('order_status') == 'RETURNED') & (F.col('quantity') >= 0)),
            'INVALID_QUANTITY',
        ),

        _reason(
            F.col('unit_price').isNull() | (F.col('unit_price') < 0),
            'INVALID_UNIT_PRICE',
        ),

        _reason(
            F.col('unit_cost').isNull() | (F.col('unit_cost') < 0),
            'INVALID_UNIT_COST',
        ),

        _reason(
            F.col('discount_amount').isNull() | (F.col('discount_amount') < 0),
            'INVALID_DISCOUNT_AMOUNT',
        ),

        _reason(
            F.col('order_status').isNull()
            | ~F.col('order_status').isin('COMPLETED', 'CANCELLED', 'RETURNED'),
            'INVALID_ORDER_STATUS',
        ),

        _reason(
            F.col('updated_at').isNull(),
            'INVALID_UPDATED_AT',
        ),

        _reason(
            (F.col('order_status') == 'COMPLETED')
            & (F.col('quantity') > 0)
            & (F.col('unit_price') >= 0)
            & (F.col('discount_amount') >= 0)
            & (F.col('discount_amount') > gross_sales),
            'DISCOUNT_EXCEEDS_GROSS_SALES',
        ),
    )

    return df.withColumn(
        'rejection_reasons',
        rejection_reasons,
    )


def add_products_rejection_reasons(df: DataFrame) -> DataFrame:
    """
    Apply reference-data validity for products.

    PRODUCTS
    - missing/blank product_id
    - missing/blank product_name
    - missing/blank category
    - invalid/unparseable active
    - duplicate product_id
    """
    product_window = Window.partitionBy('product_id')
    df = df.withColumn(
        '_product_id_count',
        F.count('*').over(product_window),
    )

    rejection_reasons = F.concat(
        _reason(
            F.col('product_id').isNull() | (F.col('product_id') == ''),
            'MISSING_PRODUCT_ID',
        ),

        _reason(
            F.col('product_name').isNull() | (F.col('product_name') == ''),
            'MISSING_PRODUCT_NAME',
        ),

        _reason(
            F.col('category').isNull() | (F.col('category') == ''),
            'MISSING_CATEGORY',
        ),

        _reason(
            F.col('active').isNull(),
            'INVALID_ACTIVE',
        ),

        _reason(
            F.col('product_id').isNotNull()
            & (F.col('product_id') != '')
            & (F.col('_product_id_count') > 1),
            'DUPLICATE_PRODUCT_ID',
        )
    )

    return (
        df
        .withColumn(
            'rejection_reasons',
            rejection_reasons,
        )
        .drop('_product_id_count')
    )


def add_stores_rejection_reasons(df: DataFrame) -> DataFrame:
    """
    Apply reference-data validity for stores.

    STORES
    - missing/blank store_id
    - missing/blank store_name
    - missing/blank city
    - invalid province
    - invalid/unparseable active
    - duplicate store_id
    """
    store_window = Window.partitionBy('store_id')
    df = df.withColumn(
        '_store_id_count',
        F.count('*').over(store_window),
    )

    rejection_reasons = F.concat(
        _reason(
            F.col('store_id').isNull() | (F.col('store_id') == ''),
            'MISSING_STORE_ID',
        ),

        _reason(
            F.col('store_name').isNull() | (F.col('store_name') == ''),
            'MISSING_STORE_NAME',
        ),

        _reason(
            F.col('city').isNull() | (F.col('city') == ''),
            'MISSING_CITY',
        ),

        _reason(
            F.col('province').isNull() | (F.col('province') == ''),
            'MISSING_PROVINCE',
        ),

        _reason(
            F.col('province').isNotNull() & (F.col('province') != '')
            & ~F.col('province').isin(
                'AB',
                'BC',
                'MB',
                'NB',
                'NL',
                'NS',
                'NT',
                'NU',
                'ON',
                'PE',
                'QC',
                'SK',
                'YT',
            ),
            'INVALID_PROVINCE',
        ),

        _reason(
            F.col('active').isNull(),
            'INVALID_ACTIVE',
        ),

        _reason(
            F.col('store_id').isNotNull() & (F.col('store_id') != '')
            & (F.col('_store_id_count') > 1),
            'DUPLICATE_STORE_ID',
        )
    )

    return (
        df
        .withColumn(
            'rejection_reasons',
            rejection_reasons,
        )
        .drop('_store_id_count')
    )


def add_sales_reference_rejection_reasons(
    sales_df: DataFrame,
    products_df: DataFrame,
    stores_df: DataFrame,
) -> DataFrame:
    """
    Append sales referential-integrity rejection reasons.

    Assumes:
    - sales_df already contains row-level rejection_reasons
    - products_df and stores_df already contain their own
      rejection_reasons
    """
    # Only reference rows that passed their own validation are allowed
    # to satisfy referential-integrity checks.
    #
    # Example:
    # A product_id may physically exist in products_df, but if that
    # product row is invalid, sales must not treat it as a trusted match.
    trusted_products_df = (
        products_df
        # Filter for valid rows.
        .filter(F.size(F.col('rejection_reasons')) == 0)
        .select('product_id')
        # Temporary marker used after the left join to distinguish:
        # matched product   -> True
        # unmatched product -> null
        .withColumn('_product_exists', F.lit(True))
    )

    # Apply the same trusted-reference principle to stores.
    trusted_stores_df = (
        stores_df
        .filter(F.size(F.col('rejection_reasons')) == 0)
        .select('store_id')
        .withColumn('_store_exists', F.lit(True))
    )

    # LEFT joins preserve every sales row.
    #
    # We do not use INNER joins because an unknown product/store is
    # itself a data-quality condition that must remain visible so the
    # row can later be quarantined with an explicit rejection reason.
    df = (
        sales_df
        .join(
            trusted_products_df,
            on='product_id',
            how='left',
        )
        .join(
            trusted_stores_df,
            on='store_id',
            how='left',
        )
    )

    # Build only the new referential-integrity reasons.
    #
    # Blank/null IDs are excluded here because row-level validation
    # already classifies those as MISSING_PRODUCT_ID / MISSING_STORE_ID.
    # We do not want the same underlying problem reported twice.
    reference_reasons = F.concat(
        _reason(
            F.col('product_id').isNotNull()
            & (F.col('product_id') != '')
            & F.col('_product_exists').isNull(),
            'UNKNOWN_PRODUCT_ID',
        ),

        _reason(
            F.col('store_id').isNotNull()
            & (F.col('store_id') != '')
            & F.col('_store_exists').isNull(),
            'UNKNOWN_STORE_ID',
        ),
    )
    return (
        df
        # Append RI failures to the row-level rejection reasons that
        # already exist; do not overwrite earlier validation results.
        .withColumn(
            'rejection_reasons',
            F.concat(
                F.col('rejection_reasons'),
                reference_reasons,
            ),
        )
        # Helper marker columns are implementation details and should
        # not leak into the validated output schema.
        .drop(
            '_product_exists',
            '_store_exists',
        )
    )


def validate_sales(
    sales_df: DataFrame,
    products_df: DataFrame,
    stores_df: DataFrame,
) -> DataFrame:
    """
    Apply the complete sales validation workflow.

    Assumes products_df and stores_df have already passed through
    their respective reference-data validation functions.
    """
    row_validated_df = add_sales_rejection_reasons(sales_df)

    return add_sales_reference_rejection_reasons(
        row_validated_df,
        products_df,
        stores_df,
    )


def split_validated_records(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Split validated records into accepted and quarantined DataFrames.
    """
    accepted_df = df.filter(
        F.size(F.col('rejection_reasons')) == 0
    )
    quarantined_df = df.filter(
        F.size(F.col('rejection_reasons')) > 0
    )

    return accepted_df, quarantined_df
