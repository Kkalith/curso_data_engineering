WITH src_medications_administered AS (
    SELECT  
        appointment_id,
        medications_administered
    FROM {{ source('hospital', 'treatments') }}
),

expanded_medications_administered as (
    select
        appointment_id,
        trim(value) as medications_administered
    from src_medications_administered,
    lateral split_to_table(medications_administered, ',')
)

select
    {{ dbt_utils.generate_surrogate_key(['appointment_id','medications_administered']) }} as patient_medications_administered,
    {{ dbt_utils.generate_surrogate_key(['appointment_id']) }} as id_patient,
    medications_administered
from expanded_medications_administered