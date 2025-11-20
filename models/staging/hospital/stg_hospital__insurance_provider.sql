with 

src_insurance_provider as (

    select insurance_provider from {{ source('hospital', 'patients') }}

),

renamed as (

    select distinct
        md5(insurance_provider) as id_insurance_provider,
        insurance_provider

    from src_insurance_provider

)

select * from renamed