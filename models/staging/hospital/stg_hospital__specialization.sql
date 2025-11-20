with 

src_specialization as (

    select specialization from {{ source('hospital', 'doctors') }}

),

renamed as (

    select distinct
        md5(specialization) as id_specialization,
        specialization

    from src_specialization

)

select * from renamed