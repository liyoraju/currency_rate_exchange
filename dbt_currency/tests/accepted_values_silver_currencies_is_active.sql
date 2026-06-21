SELECT
  is_active
FROM {{ ref('silver_currencies') }}
WHERE is_active NOT IN (TRUE, FALSE)
