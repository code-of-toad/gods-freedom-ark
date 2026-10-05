-- ============================================================================
-- P001 Retail Sales — Analytical SQL
--
-- These queries operate on:
--
--     fact_sales
--     dim_product
--     dim_store
--     dim_date
--
-- Each query is identified by:
--
--     -- name: <query_name>
--
-- analytics.py uses those names to load and execute individual queries.
-- ============================================================================


-- name: daily_sales
SELECT
    d.sale_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,

    SUM(f.gross_sales) AS gross_sales,
    SUM(f.net_sales) AS net_sales,
    SUM(f.gross_margin) AS gross_margin,

    SUM(f.quantity) AS net_units,

    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS line_count

FROM fact_sales AS f

INNER JOIN dim_date AS d
    ON f.sale_date = d.sale_date

GROUP BY
    d.sale_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name

ORDER BY
    d.sale_date
;


-- name: store_performance
SELECT
    s.store_id,
    s.store_name,
    s.city,
    s.province,

    SUM(f.net_sales) AS net_sales,
    SUM(f.gross_margin) AS gross_margin,
    SUM(f.quantity) AS net_units,

    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS line_count,

    CASE
        WHEN SUM(f.net_sales) = 0 THEN NULL
        ELSE ROUND(
            (
                SUM(f.gross_margin)
                / SUM(f.net_sales)
            ) * 100,
            2
        )
    END AS gross_margin_pct

FROM fact_sales AS f

INNER JOIN dim_store AS s
    ON f.store_id = s.store_id

GROUP BY
    s.store_id,
    s.store_name,
    s.city,
    s.province

ORDER BY
    net_sales DESC,
    s.store_id
;


-- name: product_performance
SELECT
    p.product_id,
    p.product_name,
    p.category,

    SUM(f.net_sales) AS net_sales,
    SUM(f.gross_margin) AS gross_margin,
    SUM(f.quantity) AS net_units,

    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS line_count

FROM fact_sales AS f

INNER JOIN dim_product AS p
    ON f.product_id = p.product_id

GROUP BY
    p.product_id,
    p.product_name,
    p.category

ORDER BY
    net_sales DESC,
    p.product_id
;


-- name: category_performance
SELECT
    p.category,

    SUM(f.net_sales) AS net_sales,
    SUM(f.gross_margin) AS gross_margin,
    SUM(f.quantity) AS net_units,

    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS line_count,

    CASE
        WHEN SUM(f.net_sales) = 0 THEN NULL
        ELSE ROUND(
            (
                SUM(f.gross_margin)
                / SUM(f.net_sales)
            ) * 100,
            2
        )
    END AS gross_margin_pct

FROM fact_sales AS f

INNER JOIN dim_product AS p
    ON f.product_id = p.product_id

GROUP BY
    p.category

ORDER BY
    net_sales DESC,
    p.category
;


-- name: province_performance
SELECT
    s.province,

    SUM(f.net_sales) AS net_sales,
    SUM(f.gross_margin) AS gross_margin,
    SUM(f.quantity) AS net_units,

    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS line_count

FROM fact_sales AS f

INNER JOIN dim_store AS s
    ON f.store_id = s.store_id

GROUP BY
    s.province

ORDER BY
    net_sales DESC,
    s.province
;


-- name: return_activity
SELECT
    f.sale_date,

    COUNT(*) AS return_line_count,
    COUNT(DISTINCT f.order_id) AS return_order_count,

    -SUM(f.quantity) AS returned_units,
    -SUM(f.net_sales) AS return_value,
    -SUM(f.gross_margin) AS margin_reversal

FROM fact_sales AS f

WHERE
    f.order_status = 'RETURNED'

GROUP BY
    f.sale_date

ORDER BY
    f.sale_date
;


-- name: order_value_summary
WITH order_totals AS (
    SELECT
        f.sale_date,
        f.order_id,
        SUM(f.net_sales) AS order_net_sales

    FROM fact_sales AS f

    WHERE
        f.order_status = 'COMPLETED'

    GROUP BY
        f.sale_date,
        f.order_id
)

SELECT
    sale_date,

    COUNT(*) AS completed_order_count,
    SUM(order_net_sales) AS completed_net_sales,

    CAST(
        ROUND(
            AVG(order_net_sales),
            2
        )
        AS DECIMAL(24, 2)
    ) AS average_order_value

FROM order_totals

GROUP BY
    sale_date

ORDER BY
    sale_date
;
