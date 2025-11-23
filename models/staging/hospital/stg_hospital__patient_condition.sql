with src_patients as (
    select 
        patient_id,
        chronic_conditions
    from {{ source('hospital', 'patients') }}
    where chronic_conditions is not null and chronic_conditions != 'None'
),

expanded_chronic_conditions as (
    select
        patient_id,
        trim(value) as chronic_conditions
    from src_patients,
    lateral split_to_table(chronic_conditions, ',')
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id','chronic_conditions']) }} as patient_chronic_conditions,
    {{ dbt_utils.generate_surrogate_key(['patient_id']) }} as id_patient,
    chronic_conditions
from expanded_chronic_conditions