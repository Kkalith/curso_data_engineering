with 

src_payment_method as (

    select payment_method from {{ source('hospital', 'billing') }}

),

renamed as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['payment_method']) }} as id_payment_method,
        payment_method

    from src_payment_method

)

select * from renamed