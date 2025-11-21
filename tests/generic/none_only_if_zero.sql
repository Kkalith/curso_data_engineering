{% test none_only_if_zero(model, target_column, duration_column) %}
    select *
    from {{ model }}
    where {{ target_column }} = 'None'
      and {{ duration_column }} <> 0
{% endtest %}