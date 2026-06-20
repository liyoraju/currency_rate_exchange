SELECT 
    r.date,
    r.base,
    c.name AS target_name,
    r.target,
    r.rate,
    LAG(r.rate) OVER(PARTITION BY c.iso_code ORDER BY r.date) AS previous_rate,
    ROUND(
        (r.rate - LAG(r.rate) OVER(PARTITION BY c.iso_code ORDER BY r.date))
        /LAG(r.rate) OVER(PARTITION BY c.iso_code ORDER BY r.date) * 100 ,2
    ) AS percentage_change
FROM {{ref("silver_rates")}} r JOIN {{ref("silver_currencies")}} c
ON r.target = c.iso_code




    