WITH base AS (

    SELECT
        b.id_bill,         
        {{ dbt_utils.generate_surrogate_key(['b.bill_date']) }} AS id_date,
        b.id_patient,            
        b.id_payment_method,      
        b.id_treatment,          

        -- HECHOS MEDIBLES
        b.amount_dolars,
        b.payments_days,
        b.billing_delay_days,
        b.late_fee_dolars,
        b.insurance_coverage_amount,
        b.patient_payment_amount,
        b.total_amount


    FROM {{ ref('stg_hospital__billing') }} b
)

SELECT *
FROM base
ORDER BY id_bill