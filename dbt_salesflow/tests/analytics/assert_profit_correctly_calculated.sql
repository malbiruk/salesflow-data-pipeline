-- Verify profit calculation is consistent across models

WITH
product_profits AS (
  SELECT SUM(total_profit) AS product_profit
  FROM {{ ref('product_performance') }}
),
date_profits AS (
  SELECT SUM(total_profit) AS date_profit
  FROM {{ ref('revenue_by_date') }}
)

SELECT
  product_profit,
  date_profit
FROM product_profits, date_profits
WHERE ABS(product_profit - date_profit) > 0.01
