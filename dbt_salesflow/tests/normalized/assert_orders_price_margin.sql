-- Verify price is always greater than or equal to cost (positive margin)

SELECT
  p.item_type,
  p.unit_price,
  p.unit_cost
FROM {{ ref('product') }} p
WHERE p.unit_price < p.unit_cost
