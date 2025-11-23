with 

src_cancel_reason as (

    select cancel_reason from {{ source('hospital', 'appointments') }}

),

renamed as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['cancel_reason']) }} as id_cancel_reason,
        {{ clean_string('cancel_reason') }} as cancel_reason,
                CASE 
            WHEN {{ clean_string('cancel_reason') }} IS NULL OR {{ clean_string('cancel_reason') }} = 'none' THEN 'did not cancel the appointment'
            ELSE 'canceled the appointment'
        END AS cancellation_status

    from src_cancel_reason

)

select * from renamed