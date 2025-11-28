WITH base AS (

    SELECT
        t.id_treatment,                        
        t.id_treatment_type,                     
        t.id_appointment,            
        t.cost_dolars,
        t.duration_minutes,


    FROM {{ ref('stg_hospital__treatments') }} t
)

SELECT *
FROM base
ORDER BY id_treatment
