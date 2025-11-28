WITH base AS (

    SELECT
        a.id_appointment,           
        a.id_patient,               
        a.id_doctor,                
        a.id_reason_for_visit,      
        a.id_status_appointment,   
        a.id_cancel_reason, 
        {{ dbt_utils.generate_surrogate_key(['a.date_appointment']) }} AS id_date,
        {{ dbt_utils.generate_surrogate_key(['a.time_appointment']) }} AS id_time,
        a.date_appointment,
        a.time_appointment,
        a.appointment_datetime,
        a.appointment_check_in_time,
        a.check_in_time,
        a.appointment_check_out_time,
        a.check_out_time,
        a.wait_time_minutes,
        a.duration_minutes,
        a.wait_time_hours,
        a.duration_hours

    FROM {{ ref('stg_hospital__appointments') }} a
)

SELECT *
FROM base
ORDER BY id_appointment
