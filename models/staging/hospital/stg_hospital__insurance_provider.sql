with 

src_insurance_provider as (

    select insurance_provider from {{ source('hospital', 'patients') }}

),

renamed as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['insurance_provider']) }} as id_insurance_provider,
        {{ clean_string('insurance_provider') }} as insurace_provider

    from src_insurance_provider

)

select * from renamed