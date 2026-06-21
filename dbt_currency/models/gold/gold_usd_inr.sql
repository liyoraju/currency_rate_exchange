WITH eur_inr AS (
  SELECT date, rate AS eur_to_inr 
  FROM {{ ref('gold_daily_rates') }} 
  WHERE target = 'INR'
),
eur_usd AS (
  SELECT date, rate AS eur_to_usd 
  FROM {{ ref('gold_daily_rates') }} 
  WHERE target = 'USD'
),
combined AS (
  SELECT 
    e.date,
    ROUND(e.eur_to_inr / u.eur_to_usd, 4) AS usd_to_inr
  FROM eur_inr e
  JOIN eur_usd u ON e.date = u.date
)
SELECT 
  date,
  usd_to_inr,
  LAG(usd_to_inr) OVER (ORDER BY date ASC) AS prev_usd_to_inr,
  ROUND(
    (usd_to_inr - LAG(usd_to_inr) OVER (ORDER BY date ASC)) 
    / LAG(usd_to_inr) OVER (ORDER BY date ASC) * 100, 4) AS pct_change
FROM combined