{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge',
    unique_key=['iso_code']
    )
}}

SELECT
CAST(start_date AS DATE) AS start_date,
CAST(end_date AS DATE) AS end_date,
name,
symbol,
iso_numeric,
iso_code,
CASE 
  WHEN end_date IS NULL OR end_date >= CURRENT_DATE() THEN TRUE 
  ELSE FALSE 
END AS is_active
FROM {{source('bronze','raw_currencies')}}
WHERE iso_code IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY iso_code ORDER BY start_date DESC) = 1