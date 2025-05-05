-- Verify shipping times are positive

SELECT
  order_priority,
  avg_days_to_ship
FROM {{ ref('performance_by_order_priority') }}
WHERE avg_days_to_ship < 0
