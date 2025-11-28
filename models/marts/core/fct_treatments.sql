WITH base AS (

    SELECT
        t.id_treatment,                          -- PK del hecho
        t.id_treatment_type,                     -- FK a dimensión
        t.id_appointment,                        -- Degenerado (nivel de grano)
        
        -- HECHOS MEDIBLES
        t.cost_dolars,
        t.duration_minutes,

        -- Tiempo del hecho
        {{ dbt_utils.generate_surrogate_key(['t.treatment_date']) }} AS id_date

    FROM {{ ref('stg_hospital__treatments') }} t
)

SELECT *
FROM base
ORDER BY id_treatment
