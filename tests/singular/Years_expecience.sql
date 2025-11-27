-- Test singular: years_experience debe ser <= 70

select *
from {{ source('hospital', 'doctors') }}
where years_experience > 60
