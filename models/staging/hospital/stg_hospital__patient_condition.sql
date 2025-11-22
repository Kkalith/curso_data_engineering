
with src_patients as (
    select 
        patient_id,
        chronic_conditions
    from {{ source('hospital', 'patients') }}
    where chronic_conditions is not null and chronic_conditions != 'None'
),

expanded_conditions as (
    select
        patient_id,
        trim(value) as condition_name
    from src_patients,
    lateral split_to_table(chronic_conditions, ',')
)

select
    md5(patient_id || condition_name) as patient_condition_id,
    patient_id,
    condition_name,
    current_timestamp() as loaded_at
from expanded_conditions