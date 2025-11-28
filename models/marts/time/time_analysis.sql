SELECT 
    a.id_appointment,
    d.date_day AS full_date,
    d.day_of_week,
    d.day_of_week_name,
    
    -- la hora NO está en dim_date, la sacamos del fact
    EXTRACT(HOUR FROM a.appointment_datetime) AS appointment_hour,
    
    p.id_patient,
    p.age,
    p.gender,
    p.blood_type,
    
    doc.full_name,
    doc.specialization,

    a.wait_time_minutes,
    a.duration_minutes,
    (a.wait_time_minutes + a.duration_minutes) AS total_time_minutes,
    a.check_in_time,
    a.check_out_time

FROM {{ ref('fct_appointments') }} a
LEFT JOIN {{ ref('dim_date') }} d          ON a.id_date = d.id_date
LEFT JOIN {{ ref('dim_patients') }} p      ON a.id_patient = p.id_patient
LEFT JOIN {{ ref('dim_doctors') }} doc     ON a.id_doctor = doc.id_doctor
