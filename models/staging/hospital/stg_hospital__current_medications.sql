
with src_patients as (
    select 
        patient_id,
        current_medications
    from {{ source('hospital', 'patients') }}
    where current_medications is not null and current_medications != 'None'
),

expanded_medications as (
    select
        patient_id,
        trim(value) as medication_name
    from src_patients,
    lateral split_to_table(current_medications, ',')
)

select
    md5(patient_id || medication_name) as patient_medication_id,
    patient_id,
    medication_name,
    current_timestamp() as loaded_at
from expanded_medications