-- Ensure ship dates are not in the future from the order date

SELECT
  id,
  order_date,
  ship_date
FROM {{ ref('orders') }}
WHERE ship_date < order_date
