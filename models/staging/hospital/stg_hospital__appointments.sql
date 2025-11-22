with 

src_appointments as (

    select * from {{ source('hospital', 'appointments') }}

),

renamed as (

    select
        md5(appointment_id) as id_appointment,
        try_to_date(appointment_date) as date_appointment,
        appointment_time as time_appointment,
        check_in_time,
        check_out_time,
        wait_time_minutes::integer as wait_time_minutes,
        duration_minutes::integer as duration_minutes,
        (wait_time_minutes/60)::numeric as wait_time_hours, -- tiempo esperado en horas
        (duration_minutes/60):: numeric as duration_hours, -- duración cita en horas
        case
        when check_in_time is null then 'The patient did not show up to the appointment'
        else 'The patient attended the appointment'
        end as show_up_message,
        md5(lower(trim(cancel_reason))) as id_cancel_reason,
        md5(lower(trim(reason_for_visit))) as id_reason_for_visit,
        md5(lower(trim(status))) as id_status_appointment,
        md5(patient_id) as id_patient,
        md5(doctor_id) as id_doctor

    from src_appointments

)

select * from renamed