SELECT
  date,
  target,
  base,
  COUNT(*) AS cnt
FROM {{ ref('silver_rates') }}
GROUP BY date, target, base
HAVING COUNT(*) > 1
