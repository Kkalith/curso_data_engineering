WITH payment_method AS (
    SELECT
        id_payment_method AS original_id,
        payment_method AS descripcion,
        'payment_method' AS tipo
    FROM {{ ref('stg_hospital__payment_method') }}
),

insurance_provider AS (
    SELECT
        id_insurance_provider AS original_id,
        insurance_provider AS descripcion,
        'insurance_provider' AS tipo
    FROM {{ ref('stg_hospital__insurance_provider') }}
),

unioned AS (
    SELECT * FROM payment_method
    UNION ALL
    SELECT * FROM insurance_provider
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['original_id', 'tipo']) }} AS id_pago,
    original_id,
    descripcion,
    tipo
FROM unioned
ORDER BY tipo, descripcion