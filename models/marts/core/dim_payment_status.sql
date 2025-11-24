WITH src AS (
    SELECT DISTINCT
        treatment_type
    FROM {{ ref('stg_hospital__treatments') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['treatment_type']) }} AS treatment_type_sk,
        treatment_type,
        
        -- Optional: categoría general del tratamiento
        CASE
            WHEN LOWER(treatment_type) LIKE '%scan%' THEN 'Diagnostic'
            WHEN LOWER(treatment_type) LIKE '%x-ray%' THEN 'Diagnostic'
            WHEN LOWER(treatment_type) LIKE '%therapy%' THEN 'Therapy'
            WHEN LOWER(treatment_type) LIKE '%consult%' THEN 'Consultation'
            ELSE 'Other'
        END AS treatment_category
    FROM src
)

SELECT * FROM final;