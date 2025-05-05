-- Test that channel revenue sums match total revenue

WITH channel_revenue AS (
  SELECT SUM(total_revenue) AS channel_sum
  FROM {{ ref('performance_by_channel') }}
),
total_revenue AS (
  SELECT SUM(total_revenue) AS total_sum
  FROM {{ ref('revenue_by_date') }}
)

SELECT
  channel_sum,
  total_sum
FROM channel_revenue, total_revenue
WHERE ABS(channel_sum - total_sum) > 0.01
