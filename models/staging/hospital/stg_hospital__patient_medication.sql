with src_patients as (
    select 
        patient_id,
        current_medications
    from {{ source('hospital', 'patients') }}
    where current_medications is not null and current_medications != 'None'
),

expanded_current_medications as (
    select
        patient_id,
        trim(value) as current_medications
    from src_patients,
    lateral split_to_table(current_medications, ',')
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id','current_medications']) }} as patient_current_medications,
    {{ dbt_utils.generate_surrogate_key(['patient_id']) }} as id_patient,
    current_medications
from expanded_current_medications