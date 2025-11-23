with 

source as (

    select * from {{ source('hospital', 'treatments') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['treatment_id']) }} as id_treatment,
        description,
        cost as cost_dolars,
        treatment_date,
        treatment_outcome,
        medications_administered,
        complications,
        duration_minutes,
        equipment_used,
        risk_level,
        {{ dbt_utils.generate_surrogate_key(['appointment_id']) }} as id_appointment,
        {{ dbt_utils.generate_surrogate_key(['treatment_type']) }} as id_treatment_type

    from source

)

select * from renamed