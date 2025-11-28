-- Si payment_status = 'Paid', payment_date NO puede ser NULL

select *
from {{ source('hospital', 'billing') }}
where payment_status = 'Paid'
  and payment_date is null
