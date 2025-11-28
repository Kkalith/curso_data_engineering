WITH appointments AS (
    SELECT
        id_appointment,
        id_status_appointment,
        id_cancel_reason,
        id_reason_for_visit,
    FROM {{ ref('stg_hospital__appointments') }}
),

status AS (
    SELECT
        id_status_appointment,
        status,
        appointment_category
    FROM {{ ref('stg_hospital__status_appointment') }}
),

cancel AS (
    SELECT
        id_cancel_reason,
        cancel_reason,
        is_cancelation
    FROM {{ ref('stg_hospital__cancel_reason') }}
),

reason AS (
    SELECT
        id_reason_for_visit,
        reason_for_visit
    FROM {{ ref('stg_hospital__reason_for_visit') }}
)

SELECT
    a.id_appointment,
    s.status,
    s.appointment_category,

    c.cancel_reason,
    c.is_cancelation,

    r.reason_for_visit

FROM appointments a
LEFT JOIN status s
    ON a.id_status_appointment = s.id_status_appointment
LEFT JOIN cancel c
    ON a.id_cancel_reason = c.id_cancel_reason
LEFT JOIN reason r
    ON a.id_reason_for_visit = r.id_reason_for_visit