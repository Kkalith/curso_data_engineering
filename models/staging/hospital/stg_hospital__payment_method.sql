with 

src_payment_method as (

    select payment_method from {{ source('hospital', 'billing') }}

),

renamed as (

    select distinct
        md5(payment_method) as id_payment_method,
        payment_method

    from src_payment_method

)

select * from renamed