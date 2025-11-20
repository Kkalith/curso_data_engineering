with 

src_blood_type as (
    select blood_type from {{ source('hospital', 'patients') }}
),

renamed as (
    select distinct
        md5(blood_type) as id_blood_type,
        blood_type,
        case 
            when blood_type like 'A%' then 'A'
            when blood_type like 'B%' then 'B'
            when blood_type like 'AB%' then 'AB'
            when blood_type like 'O%' then 'O'
        end as blood_group,
        case 
            when blood_type like '%+' then 'Rh+'
            when blood_type like '%-' then 'Rh-'
        end as rh_factor
    from src_blood_type
)

select * from renamed