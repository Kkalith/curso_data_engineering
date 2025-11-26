WITH base AS (

    SELECT
        a.id_appointment,           -- PK
        a.id_patient,               -- FK
        a.id_doctor,                -- FK
        a.id_reason_for_visit,      -- FK
        a.id_status_appointment,    -- FK
        {{ dbt_utils.generate_surrogate_key(['a.date_appointment']) }} AS id_date,
        {{ dbt_utils.generate_surrogate_key(['a.time_appointment']) }} AS id_time,
        a.id_cancel_reason, 
        a.date_appointment,
        a.time_appointment,
        a.appointment_datetime,
        check_in_time,

        
                -- 

        a.wait_time_minutes,
        a.duration_minutes,
        a.wait_time_hours,
        a.duration_hours

    FROM {{ ref('stg_hospital__appointments') }} a
)

SELECT *
FROM base
ORDER BY id_appointment

--RELACIONAR CON DIM_PAGO