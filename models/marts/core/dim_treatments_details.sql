WITH src AS (
    SELECT
        t.id_treatment,
        t.description,
        t.cost_dolars,
        t.treatment_date,
        t.treatment_outcome,
        t.complications,
        t.duration_minutes,
        t.equipment_used,
        t.risk_level,

        -- denormalized lookup
        tt.treatment_type,

        -- keep appointment context
        t.id_appointment,

        ------------------------------
        -- (1) DATE DERIVED ATTRIBUTES
        ------------------------------
        DATE_PART(year, t.treatment_date)  AS treatment_year,
        DATE_PART(month, t.treatment_date) AS treatment_month,
        DATE_PART(week, t.treatment_date)  AS treatment_week,
        CASE 
            WHEN DATE_PART(dow, t.treatment_date) IN (0,6) 
            THEN 'weekend' 
            ELSE 'weekday' 
        END AS treatment_day_type,

        ------------------------------
        -- (3) FLAGS
        ------------------------------
        CASE 
            WHEN t.complications IS NULL OR t.complications = '' 
            THEN 0 ELSE 1 
        END AS has_complications,

        CASE 
            WHEN t.risk_level IN ('high','critical') 
            THEN 1 ELSE 0 
        END AS high_risk_flag,

        CASE 
            WHEN t.treatment_outcome = 'unsuccessful' 
            THEN 1 ELSE 0 
        END AS unsuccessful_flag,

        ------------------------------
        -- (8) EQUIPMENT COUNT
        ------------------------------
        CASE 
            WHEN t.equipment_used IS NULL OR t.equipment_used = '' THEN 0
            ELSE LENGTH(t.equipment_used) - LENGTH(REPLACE(t.equipment_used, ',', '')) + 1
        END AS num_equipment_items

    FROM {{ ref('stg_hospital__treatments') }} t
    LEFT JOIN {{ ref('stg_hospital__treatment_type') }} tt
        ON t.id_treatment_type = tt.id_treatment_type
)

SELECT * FROM src
