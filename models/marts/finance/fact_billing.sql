-- ============================================================
-- FACT BILLING
-- ============================================================
-- Este modelo integra los datos ya normalizados en staging
-- para construir la tabla de hechos principal del sistema.
-- ============================================================

WITH billing AS (
    SELECT *
    FROM {{ ref('stg_hospital__billing') }}
),

dim_patients AS (
    SELECT *
    FROM {{ ref('dim_patients') }}
),

dim_treatments AS (
    SELECT *
    FROM {{ ref('dim_treatments') }}
),

dim_payment_methods AS (
    SELECT *
    FROM {{ ref('dim_payment_method') }}
),

dim_date AS (
    SELECT *
    FROM {{ ref('dim_date') }}
),

-- ============================================================
-- ENRIQUECER BILLING CON DIMENSIONES
-- ============================================================

final AS (
    SELECT
        b.id_bill,
        -- FKs a dimensiones
        p.id_patient,
        --t.id_treatment_type,
        pm.id_payment_method,
        dd.id_date,
        t.id_treatment,


        -- Métricas del hecho
        b.amount_dolars,
        b.payment_status,
        b.is_payed,
        b.payments_days,
        b.billing_delay_days,
        b.is_late,
        b.late_fee_dolars,
        b.insurance_coverage_amount,
        b.patient_payment_amount,
        b.total_amount,

        -- Fechas
        b.bill_date,
        b.payment_date,
        b.due_date


    FROM billing b
    LEFT JOIN dim_patients        p   ON p.id_patient = b.id_patient
    --LEFT JOIN dim_treatment_type  t   ON t.id_treatment_type = b.id_treatment
    LEFT JOIN dim_payment_methods pm  ON pm.id_payment_method = b.id_payment_method
    LEFT JOIN dim_date        dd  ON dd.date_day = b.bill_date
    LEFT JOIN dim_treatments t ON t.id_treatment = b.id_treatment

    --{% if is_incremental() %}
      --WHERE b.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    --{% endif %}
)

SELECT *
FROM final