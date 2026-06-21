SELECT
  g.target
FROM {{ ref('gold_daily_rates') }} g
LEFT JOIN {{ ref('silver_currencies') }} c
  ON g.target = c.iso_code
WHERE c.iso_code IS NULL
