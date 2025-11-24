-- ============================
-- 1. BASE PATIENTS TABLE
-- ============================
WITH patients AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__patients') }}
),

-- ============================
-- 2. BLOOD TYPE LOOKUP
-- ============================
blood_type AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__blood_type') }}
),

-- ============================
-- 3. INSURANCE PROVIDER LOOKUP
-- ============================
insurance AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__insurance_provider') }}
),

-- ============================
-- 4. ALLERGIES (1:N → agrega)
-- ============================
allergies AS (
    SELECT
        id_patient,
        LISTAGG(allergy_name, ', ')
            WITHIN GROUP (ORDER BY allergy_name) AS allergies
    FROM {{ ref('stg_hospital__patient_allergy') }}
    GROUP BY id_patient
),

-- ============================
-- 5. CHRONIC CONDITIONS (1:N)
-- ============================
conditions AS (
    SELECT
        id_patient,
        LISTAGG(chronic_conditions, ', ')
            WITHIN GROUP (ORDER BY chronic_conditions) AS chronic_conditions
    FROM {{ ref('stg_hospital__patient_condition') }}
    GROUP BY id_patient
),

-- ============================
-- 6. MEDICATIONS (1:N)
-- ============================
medications AS (
    SELECT
        id_patient,
        LISTAGG(current_medications, ', ')
            WITHIN GROUP (ORDER BY current_medications) AS current_medications
    FROM {{ ref('stg_hospital__patient_medication') }}
    GROUP BY id_patient
),

-- ============================
-- 7. FINAL JOIN
-- ============================
final AS (
    SELECT
        p.id_patient,
        INITCAP(p.first_name || ' ' || p.last_name) AS full_name,
        p.first_name,
        p.last_name,
        p.gender,
        p.date_of_birth,
        p.age,
        CASE
            WHEN p.age < 18 THEN 'Child'
            WHEN p.age BETWEEN 18 AND 39 THEN 'Adult'
            WHEN p.age BETWEEN 40 AND 64 THEN 'Middle Age'
            ELSE 'Senior'
        END AS age_group,
        p.contact_number,
        p.address,
        p.street_name,
        p.registration_date,
        DATE_PART('year', p.registration_date) AS registration_year,
        DATE_PART('month', p.registration_date) AS registration_month,
        DATE_PART('quarter', p.registration_date) AS registration_quarter,
        p.insurance_number,
        p.email,
        p.medical_history_summary,
        ins.insurance_provider,
        bt.blood_type,
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
