with 

src_appointments as (

    select * from {{ source('hospital', 'appointments') }}

),

renamed as (

    select
        appointment_id,
        patient_id,
        doctor_id,
        appointment_date,
        appointment_time,
        reason_for_visit,
        status,
        check_in_time,
        check_out_time,
        wait_time_minutes,
        duration_minutes,
        cancel_reason

    from source

)

select * from renamed