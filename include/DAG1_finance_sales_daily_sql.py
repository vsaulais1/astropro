AGG_SQL_TEMPLATE ="""
WITH oi AS (
  SELECT
    DATE(created_at) AS order_date,
    inventory_item_id,
    product_id,
    user_id,
    CAST(sale_price AS NUMERIC) AS sale_price
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  WHERE DATE(created_at) >= DATE(@start_date)
    AND DATE(created_at) <  DATE(@end_date)
),
enriched AS (
  SELECT
    oi.order_date,
    dc.id AS distribution_center_id,
    dc.name AS distribution_center,
    p.category,
    p.department,
    oi.sale_price
  FROM oi
  JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` ii
    ON oi.inventory_item_id = ii.id
  JOIN `bigquery-public-data.thelook_ecommerce.distribution_centers` dc
    ON ii.product_distribution_center_id = dc.id
  JOIN `bigquery-public-data.thelook_ecommerce.products` p
    ON oi.product_id = p.id
  JOIN `bigquery-public-data.thelook_ecommerce.users` u
    ON oi.user_id = u.id
)
SELECT
  order_date,
  distribution_center_id,
  distribution_center,
  category,
  department,
  --COUNT(1) AS units,
  SUM(sale_price) AS gross_sales,
  --AVG(sale_price) AS avg_unit_price
FROM enriched
GROUP BY 1,2,3,4,5
ORDER BY 1,2,4,5;
"""