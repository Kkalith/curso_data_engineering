SELECT 
    d.date_day AS appointment_date,
    APPROX_PERCENTILE(a.wait_time_minutes, 0.5) AS p50_wait,
    APPROX_PERCENTILE(a.wait_time_minutes, 0.75) AS p75_wait,
    APPROX_PERCENTILE(a.wait_time_minutes, 0.95) AS p95_wait
FROM {{ ref('fct_appointments') }} a
JOIN {{ ref('dim_date') }} d
    ON a.id_date = d.id_date
GROUP BY d.date_day
ORDER BY d.date_day