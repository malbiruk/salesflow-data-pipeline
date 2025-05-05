-- Test that the incremental model captures all orders

WITH
raw_order_count AS (
  SELECT COUNT(DISTINCT order_id) AS raw_count
  FROM {{ ref('raw_sales_data_clean') }}
),
normalized_order_count AS (
  SELECT COUNT(DISTINCT source_order_id) AS normalized_count
  FROM {{ ref('orders') }}
)

SELECT
  raw_count,
  normalized_count
FROM raw_order_count, normalized_order_count
WHERE raw_count != normalized_count
