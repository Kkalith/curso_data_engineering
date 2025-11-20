with 

src_reason_for_visit as (

    select reason_for_visit from {{ source('hospital', 'appointments') }}

),

renamed as (

    select distinct
        md5(reason_for_visit) as id_reason_for_visit,
        reason_for_visit

    from src_reason_for_visit

)

select * from renamed