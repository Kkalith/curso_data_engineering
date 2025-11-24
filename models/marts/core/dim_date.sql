WITH base AS (

    -- Genera toda la dimensión fecha
    {{ dbt_date.get_date_dimension("2020-01-01", "2040-12-31") }}

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS id_date,  -- nuevo ID único
    *
FROM base
ORDER BY date_day