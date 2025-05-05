-- Verify deduplication in raw_sales_data_clean

SELECT
  order_id,
  COUNT(*) as record_count
FROM {{ ref('raw_sales_data_clean') }}
GROUP BY order_id
HAVING COUNT(*) > 1
