-- -----------------------------------------------------------
-- Verifica que todas las citas con status = 'Completed'
-- tengan un check_in_time no nulo.
-- -----------------------------------------------------------

SELECT *
FROM {{ source('hospital','appointments') }}
WHERE status = 'Completed'
  AND check_in_time IS NULL