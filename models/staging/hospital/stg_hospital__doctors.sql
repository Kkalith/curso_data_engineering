with 

src_doctors as (

    select * from {{ source('hospital', 'doctors') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['doctor_id']) }} as id_doctor,
        first_name,
        last_name,
        phone_number,
        years_experience,
        email,
        consultation_fee,
        office_room,
        {{ dbt_utils.generate_surrogate_key(['specialization']) }} as id_specialization,
        {{ dbt_utils.generate_surrogate_key(['hospital_branch']) }} as id_hospital_branch,
        {{ dbt_utils.generate_surrogate_key(['available_days']) }} as id_available_days

    from src_doctors

)

select * from renamed