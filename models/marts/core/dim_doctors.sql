WITH doctors AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__doctors') }}
),

specialization AS (
    SELECT
    *
    FROM {{ ref('stg_hospital__specialization') }}
),

branch AS (
    SELECT
        *
    FROM {{ ref('stg_hospital__hospital_branch') }}
),

available_days AS (
    SELECT
        id_available_days,
        LISTAGG(full_day_name, ', ')
            WITHIN GROUP (ORDER BY full_day_name) AS available_days
    FROM {{ ref('stg_hospital__available_days') }}
    GROUP BY id_available_days
),

final AS (
    SELECT
        d.id_doctor,
        d.full_name,
        d.office_room,
        d.phone_number,
        d.email,
        d.years_experience,
        d.consultation_fee,

        s.specialization,
        b.hospital_branch,
        ad.available_days,

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

        AVG(d.consultation_fee) OVER (
            PARTITION BY s.specialization
        ) AS avg_fee_in_specialty,

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