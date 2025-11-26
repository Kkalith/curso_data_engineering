with 

src_cancel_reason as (

    select cancel_reason 
    from {{ source('hospital', 'appointments') }}

),

renamed as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['cancel_reason']) }} as id_cancel_reason,
        {{ clean_string('cancel_reason') }} as cancel_reason,
        (
            not (
                {{ clean_string('cancel_reason') }} is null
                or {{ is_in(clean_string('cancel_reason'), ['None']) }}
            )
        ) as is_cancelation
    from src_cancel_reason

)

select * from renamed