{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id_bill'
) }}

with 

src_billing as (
    select *
    from {{ source('hospital', 'billing') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['bill_id']) }} as id_bill,
        bill_date,
        coalesce(amount, 0) as amount_dolars,
        {{ clean_string('payment_status') }} as payment_status,
        {{ is_in(clean_string('payment_status'), ['Paid']) }} as is_payed,
        payment_date,
        due_date,
        datediff(day, due_date, payment_date) as payments_days,
        datediff(day, bill_date, payment_date) as billing_delay_days,
        case 
            when payment_date is not null 
                 and due_date is not null 
                 and payment_date > due_date 
            then true 
            else false 
        end as is_late,
        coalesce(late_fee, 0) as late_fee_dolars,
        coalesce(insurance_coverage_amount, 0) as insurance_coverage_amount,
        coalesce(patient_payment_amount, 0) as patient_payment_amount,
        coalesce(patient_payment_amount, 0) + coalesce(insurance_coverage_amount, 0) as total_amount,
        {{ dbt_utils.generate_surrogate_key(['patient_id']) }} as id_patient,
        {{ dbt_utils.generate_surrogate_key(['treatment_id']) }} as id_treatment,
        {{ dbt_utils.generate_surrogate_key(['payment_method']) }} as id_payment_method
    from src_billing
)

select * 
from renamed

    {% if is_incremental() %}
        where bill_date > (select max(bill_date) from {{ this }})
    {% endif %}