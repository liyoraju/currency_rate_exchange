{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'target', 'base']
) }}

SELECT
  CAST(date AS DATE) as date,
  base,
  quote as target,
  CAST(rate AS FLOAT64) as rate
FROM {{ source('bronze', 'raw_rates') }}
WHERE date IS NOT NULL 
  AND rate > 0
