with 

src_hospital_branch as (

    select hospital_branch from {{ source('hospital', 'doctors') }}

),

renamed as (

    select distinct
       {{ dbt_utils.generate_surrogate_key(['hospital_branch']) }} as id_hospital_branch,
        hospital_branch

    from src_hospital_branch

)

select * from renamed