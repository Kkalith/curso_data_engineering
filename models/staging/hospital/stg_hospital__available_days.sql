with src_available_days as (
    select distinct available_days 
    from {{ source('hospital', 'doctors') }}
    where available_days is not null
),

day_mapping as (
    select * from (
        values
        ('Mon', 'Monday', 1),
        ('Tue', 'Tuesday', 2),
        ('Wed', 'Wednesday', 3),
        ('Thu', 'Thursday', 4),
        ('Fri', 'Friday', 5),
        ('Sat', 'Saturday', 6),
        ('Sun', 'Sunday', 7)
    ) as t(day_abbr, day_name, day_order)
),

expanded_ranges as (
    select
        available_days as original_range,
        trim(value) as day_range
    from src_available_days,
    lateral split_to_table(available_days, ',')
),

-- Identificar si es un rango continuo o días específicos
categorized_ranges as (
    select
        original_range,
        day_range,
        case 
            when array_size(split(day_range, '-')) = 2 then 'range'
            when array_size(split(day_range, '-')) > 2 then 'specific_days'
            else 'single_day'
        end as range_type,
        split(day_range, '-') as days_array
    from expanded_ranges
),

final_expanded as (
    -- Para rangos continuos (ej: "Mon-Thu")
    select
        cr.original_range,
        dm.day_name as full_day_name
    from categorized_ranges cr
    cross join day_mapping dm
    left join day_mapping dm_start on dm_start.day_abbr = cr.days_array[0]
    left join day_mapping dm_end on dm_end.day_abbr = cr.days_array[1]
    where cr.range_type = 'range'
      and dm.day_order between dm_start.day_order and dm_end.day_order
    
    union all
    
    -- Para días específicos (ej: "Mon-Wed-Fri")
    select
        cr.original_range,
        dm.day_name as full_day_name
    from categorized_ranges cr
    cross join table(flatten(input => cr.days_array)) as day_value
    join day_mapping dm on dm.day_abbr = trim(day_value.value)
    where cr.range_type = 'specific_days'
    
    union all
    
    -- Para días individuales (ej: "Mon")
    select
        cr.original_range,
        dm.day_name as full_day_name
    from categorized_ranges cr
    join day_mapping dm on dm.day_abbr = cr.day_range
    where cr.range_type = 'single_day'
)

select 
    original_range,
    full_day_name
from final_expanded
order by original_range, 
         case full_day_name
             when 'Monday' then 1
             when 'Tuesday' then 2
             when 'Wednesday' then 3
             when 'Thursday' then 4
             when 'Friday' then 5
             when 'Saturday' then 6
             when 'Sunday' then 7
         end