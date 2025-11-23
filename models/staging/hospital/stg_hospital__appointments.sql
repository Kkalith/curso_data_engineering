with 

src_appointments as (

    select * from {{ source('hospital', 'appointments') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['appointment_id']) }} as id_appointment,
        
        appointment_date as date_appointment, --castear
        appointment_time as time_appointment, --castear
        check_in_time, --castear
        check_out_time, --castear
        timestamp_from_parts(date_appointment, time_appointment) as appointment_datetime,
        --(case when check_in_time is not null then true else false end) as attended_flag,(case when status = 'no-show' then true else false end) as no_show_flag,
        --(case when cancel_reason != 'none' then true else false end) as cancel_flag,

        wait_time_minutes::integer as wait_time_minutes,
        duration_minutes::integer as duration_minutes,
        ROUND(wait_time_minutes::numeric / 60, 2) AS wait_time_hours, -- tiempo esperado en horas
        ROUND(duration_minutes::numeric / 60, 2) AS duration_hours, -- duración cita en horas
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