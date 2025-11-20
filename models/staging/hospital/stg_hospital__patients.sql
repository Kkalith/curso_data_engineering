with 

src_patients as (

    select * from {{ source('hospital', 'patients') }}

),

renamed as (

    select
        md5(patient_id) as id_patient,
        first_name,
        last_name,
        NULL AS last_name_2,
        gender, -- normalizar?
        date_of_birth,
        floor(datediff(month, date_of_birth, current_date()) / 12) as age,
        contact_number,
        address,
        regexp_replace(address, '[^a-zA-Z ]', '') as street_name,
        registration_date,
        insurance_number,
        email,
        allergies,
        chronic_conditions,
        current_medications,
        medical_history_summary,
        md5(insurance_provider) as id_insurance_provider,
        md5(blood_type) as id_blood_type

    from src_patients

)

select * from renamed