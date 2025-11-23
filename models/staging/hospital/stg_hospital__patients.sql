with 

src_patients as (

    select * from {{ source('hospital', 'patients') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['patient_id']) }} as id_patient,
        first_name, --castear
        last_name, --castear
        gender, --castear
        date_of_birth,
        floor(datediff(month, date_of_birth, current_date()) / 12) as age,
        contact_number,
        address,
        regexp_replace(address, '[^a-zA-Z ]', '') as street_name,
        registration_date,
        insurance_number,
        email,
        chronic_conditions, --MD5
        current_medications, --MD5
        medical_history_summary::TEXT AS medical_history_summary,
       {{ dbt_utils.generate_surrogate_key(['insurance_provider']) }} as id_insurance_provider,
        {{ dbt_utils.generate_surrogate_key(['blood_type']) }} as id_blood_type

    from src_patients

)

select * from renamed