with 

src_status_appointments as (

    select status from {{ source('hospital', 'appointments') }}

),

renamed AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['status']) }} AS id_status_appointment,
        status,

        CASE
            WHEN status IN ('No-show', 'Cancelled') 
                THEN 'Missed appointment'
            WHEN status = 'Completed' 
                THEN 'Attended appointment'
            WHEN status = 'Scheduled' 
                THEN 'Upcoming appointment'
            ELSE 'unknown'
        END AS appointment_category

    FROM src_status_appointments
)

select * from renamed