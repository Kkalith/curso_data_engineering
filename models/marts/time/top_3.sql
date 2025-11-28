WITH base AS (
    SELECT
        a.id_doctor,
        d.full_name,
        d.specialization,
        AVG(a.wait_time_minutes) AS avg_wait
    FROM {{ ref('stg_hospital__appointments') }} a
    LEFT JOIN {{ ref('dim_doctors') }} d
        ON a.id_doctor = d.id_doctor
    WHERE a.wait_time_minutes IS NOT NULL
    GROUP BY 1,2,3
),

-- TOP 3 con MAYOR tiempo de espera
top3_max AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY avg_wait DESC) AS rn
    FROM base
)

SELECT
    'TOP_3_MAX' AS category,
    id_doctor,
    full_name,
    specialization,
    avg_wait
FROM top3_max
WHERE rn <= 3