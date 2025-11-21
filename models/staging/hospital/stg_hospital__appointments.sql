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
        wait_time_minutes as wait_time_minutes, -- queremos que sea null para no interferir en nuestras medias
        duration_minutes as duration_minutes, -- queremos que sea null los 0 para no interferir en nuestras medias
        wait_time_minutes/60 as wait_time_hours, -- tiempo esperado en horas
        duration_minutes/60 as duration_hours, -- duración cita en horas
        md5(cancel_reason) as id_cancel_reason,
        md5(reason_for_visit) as id_reason_for_visit,
        md5(status) as id_status_appointment,
        md5(patient_id) as id_patient,
        md5(doctor_id) as id_doctor

    from src_appointments

)

select * from renamed