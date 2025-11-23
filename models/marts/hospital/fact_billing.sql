with base as (
    select
        id_bill,
    from {{ ref('stg_hospital__billing') }}
)

select *
from base