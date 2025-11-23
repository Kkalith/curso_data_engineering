with 

src_specialization as (

    select specialization from {{ source('hospital', 'doctors') }}

),

renamed as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['specialization']) }} as id_specialization,
        specialization

    from src_specialization

)

select * from renamed