with 

src_doctors as (

    select * from {{ source('hospital', 'doctors') }}

),

renamed as (

    select
        md5(doctor_id) as id_doctor,
        first_name,
        last_name,
        NULL AS last_name_2,
        phone_number,
        years_experience,
        hospital_branch,
        email,
        consultation_fee,
        available_days,
        office_room,
        md5(specialization) as id_specialization

    from src_doctors

)

select * from renamed