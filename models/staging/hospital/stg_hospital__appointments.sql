with 

src_appointments as (

    select * from {{ source('hospital', 'appointments') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['appointment_id']) }} as id_appointment,
        appointment_date as date_appointment, 
        appointment_time as time_appointment,
        timestamp_from_parts(date_appointment, time_appointment) as appointment_datetime,
        check_in_time,
        timestamp_from_parts(date_appointment, check_in_time) as appointment_check_in_time,
        check_out_time, 
        timestamp_from_parts(date_appointment, check_out_time) as appointment_check_out_time,
        wait_time_minutes,
        duration_minutes,
        ROUND(wait_time_minutes::numeric / 60, 2) AS wait_time_hours,
        ROUND(duration_minutes::numeric / 60, 2) AS duration_hours,
        case
        when check_in_time is null then 'The patient did not show up to the appointment'
        else 'The patient attended the appointment'
        end as show_up_message,
        {{ dbt_utils.generate_surrogate_key(['cancel_reason']) }} as id_cancel_reason,
        {{ dbt_utils.generate_surrogate_key(['reason_for_visit']) }} as id_reason_for_visit,
        {{ dbt_utils.generate_surrogate_key(['status']) }} as id_status_appointment,
        {{ dbt_utils.generate_surrogate_key(['patient_id']) }} as id_patient,
        {{ dbt_utils.generate_surrogate_key(['doctor_id']) }} as id_doctor

    from src_appointments

)

select * from renamed