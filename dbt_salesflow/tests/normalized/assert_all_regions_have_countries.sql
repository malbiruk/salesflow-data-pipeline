-- Make sure no regions exist without associated countries

SELECT
  r.region
FROM {{ ref('region') }} r
LEFT JOIN {{ ref('country') }} c ON r.region = c.region
WHERE c.country IS NULL
