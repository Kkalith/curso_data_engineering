WITH patients AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__patients') }}
),

blood_type AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__blood_type') }}
),

insurance AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__insurance_provider') }}
),

allergies AS (
    SELECT
        id_patient,
        LISTAGG(allergy_name, ', ')
            WITHIN GROUP (ORDER BY allergy_name) AS allergies
    FROM {{ ref('stg_hospital__patient_allergy') }}
    GROUP BY id_patient
),

conditions AS (
    SELECT
        id_patient,
        LISTAGG(chronic_conditions, ', ')
            WITHIN GROUP (ORDER BY chronic_conditions) AS chronic_conditions
    FROM {{ ref('stg_hospital__patient_condition') }}
    GROUP BY id_patient
),

medications AS (
    SELECT
        id_patient,
        LISTAGG(current_medications, ', ')
            WITHIN GROUP (ORDER BY current_medications) AS current_medications
    FROM {{ ref('stg_hospital__patient_medication') }}
    GROUP BY id_patient
),

final AS (
    SELECT
        p.id_patient,
        p.full_name,
        p.gender,
        p.date_of_birth,
        p.age,
        CASE
            WHEN p.age < 18 THEN 'Child'
            WHEN p.age BETWEEN 18 AND 39 THEN 'Adult'
            WHEN p.age BETWEEN 40 AND 64 THEN 'Middle Age'
            ELSE 'Senior'
        END AS age_group,
        p.street_name,
        p.registration_date,
        p.insurance_number,
        p.medical_history_summary,
        ins.insurance_provider,
        bt.blood_type,
        bt.blood_group,
        bt.rh_factor,
        a.allergies,
        c.chronic_conditions,
        m.current_medications

    FROM patients p
    LEFT JOIN blood_type bt USING (id_blood_type)
    LEFT JOIN insurance ins USING (id_insurance_provider)
    LEFT JOIN allergies a ON a.id_patient = p.id_patient
    LEFT JOIN conditions c ON c.id_patient = p.id_patient
    LEFT JOIN medications m ON m.id_patient = p.id_patient
)

SELECT * FROM final