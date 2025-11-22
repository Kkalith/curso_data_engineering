-- models/staging/hospital/stg_patient_allergies.sql
with src_patients as (
    select 
        patient_id,
        allergies
    from {{ source('hospital', 'patients') }}
    where allergies is not null and allergies != 'None'
),

expanded_allergies as (
    select
        patient_id,
        trim(value) as allergy_name
    from src_patients,
    lateral split_to_table(allergies, ',')
)

select
    md5(cast(patient_id as string) || cast(allergy_name as string)) as patient_allergy_id
,
    patient_id,
    allergy_name
from expanded_allergies