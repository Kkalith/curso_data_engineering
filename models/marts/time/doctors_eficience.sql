SELECT 
    doc.full_name,
    doc.specialization,
    COUNT(*) AS total_citas,
    AVG(a.wait_time_minutes) AS avg_wait,
    AVG(a.duration_minutes) AS avg_duration,
    AVG(a.wait_time_minutes + a.duration_minutes) AS avg_total
FROM {{ ref('fct_appointments') }} a
JOIN {{ ref('dim_doctors') }} doc ON a.id_doctor = doc.id_doctor
GROUP BY 1,2
ORDER BY avg_total ASC