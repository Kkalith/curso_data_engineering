with 

src_reason_for_visit as (

    select reason_for_visit from {{ source('hospital', 'appointments') }}

),

renamed as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['reason_for_visit']) }} as id_reason_for_visit,
       {{ clean_string('reason_for_visit') }} as reason_for_visit,
       CASE 
            WHEN {{ clean_string('reason_for_visit') }} IN ('emergency') THEN 'urgent'
            WHEN {{ clean_string('reason_for_visit') }} IN ('consultation','follow-up') THEN 'routine'
            WHEN {{ clean_string('reason_for_visit') }} IN ('therapy') THEN 'treatment'
            ELSE 'other'
        END AS reason_category

    from src_reason_for_visit

)

select * from renamed