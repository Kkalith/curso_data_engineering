with 

src_status_appointments as (

    select status from {{ source('hospital', 'appointments') }}

),

renamed as (

    select distinct
        md5(status) as id_status_appointment,
        status

    from src_status_appointments

)

select * from renamed