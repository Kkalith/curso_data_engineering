with 

src_billing as (

    select * from {{ source('hospital', 'billing') }}

),

renamed as (

    select
        md5(bill_id) as id_bill,
        bill_date,
        amount as amount_dolars,
        payment_status, -- normalizar??
        payment_date,
        due_date, -- fecha vencimiento
        DATEDIFF(day, payment_date, due_date) AS payments_days, -- fecha de retrado entre pago y fecha vencimiento
        late_fee as late_fee_dolars, 
        insurance_coverage_amount,
        patient_payment_amount,
        (patient_payment_amount - insurance_coverage_amount) as diference_patient_insurance,
        md5(patient_id) as id_patient,
        md5(treatment_id) as id_treatment,
        md5(payment_method) as id_payment_method

    from src_billing

)

select * from renamed