-- Verify region-country integrity

WITH
analytics_countries AS (
  SELECT region, COUNT(DISTINCT country) AS country_count
  FROM {{ ref('performance_by_region') }}
  GROUP BY region
),
normalized_countries AS (
  SELECT r.region, COUNT(DISTINCT c.country) AS country_count
  FROM {{ ref('region') }} r
  JOIN {{ ref('country') }} c ON r.region = c.region
  GROUP BY r.region
)

SELECT
  a.region,
  a.country_count AS analytics_count,
  n.country_count AS normalized_count
FROM analytics_countries a
JOIN normalized_countries n ON a.region = n.region
WHERE a.country_count != n.country_count
