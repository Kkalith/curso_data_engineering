{{ config(materialized="table") }}

-- 1. DOCTORS BASE TABLE
WITH doctors AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__doctors') }}
),

-- 2. SPECIALIZATION (1:1)
specialization AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__specialization') }}
),

-- 3. HOSPITAL BRANCH (1:1)
branch AS (
    SELECT
        *
    FROM {{ ref('stg_hospital__hospital_branch') }}
),

-- 4. AVAILABLE DAYS (1:N → agregación)
available_days AS (
    SELECT
        id_available_days,
        LISTAGG(full_day_name, ', ')
            WITHIN GROUP (ORDER BY full_day_name) AS available_days
    FROM {{ ref('stg_hospital__available_days') }}
    GROUP BY id_available_days
),

-- 5. FINAL JOIN
final AS (
    SELECT
        d.id_doctor,
        INITCAP(d.first_name || ' ' || d.last_name) AS full_name,
        d.first_name,
        d.last_name,
        d.phone_number,
        d.email,
        d.office_room,
        d.years_experience,
        d.consultation_fee,

        -- LOOKUPS
        s.specialization,
        b.hospital_branch,
        ad.available_days,

        -- ⭐ 1. Ranking por experiencia dentro de la especialidad
        RANK() OVER (
            PARTITION BY s.specialization
            ORDER BY d.years_experience DESC
        ) AS experience_rank_in_specialty,
        
        CASE 
            WHEN d.years_experience < 5 THEN 'Junior'
            WHEN d.years_experience BETWEEN 5 AND 15 THEN 'Mid'
            WHEN d.years_experience BETWEEN 16 AND 30 THEN 'Senior'
            ELSE 'Expert'
        END AS experience_level,

        -- ⭐ 3. Bucket de coste (barato/medio/caro dentro de la especialidad)
        CASE 
            WHEN NTILE(3) OVER (
                    PARTITION BY s.specialization
                    ORDER BY d.consultation_fee
                ) = 1 THEN 'Low'
            WHEN NTILE(3) OVER (
                    PARTITION BY s.specialization
                    ORDER BY d.consultation_fee
                ) = 2 THEN 'Medium'
            WHEN NTILE(3) OVER (
                    PARTITION BY s.specialization
                    ORDER BY d.consultation_fee
                ) = 3 THEN 'High'
        END AS consultation_fee_category,

        -- ⭐ 4. Media del coste por especialidad
        AVG(d.consultation_fee) OVER (
            PARTITION BY s.specialization
        ) AS avg_fee_in_specialty,

        -- ⭐ 5. Diferencia contra la media
        d.consultation_fee
        - AVG(d.consultation_fee) OVER (
            PARTITION BY s.specialization
        ) AS fee_vs_specialty_mean

    FROM doctors d
    LEFT JOIN specialization s USING (id_specialization)
    LEFT JOIN branch b USING (id_hospital_branch)
    LEFT JOIN available_days ad USING (id_available_days)
)

SELECT * FROM final
