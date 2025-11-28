with 

blood_type as (

    select * from {{ ref('stg_hospital__blood_type') }}

),

renamed as (

    select *

    from blood_type

)

select * from renamed