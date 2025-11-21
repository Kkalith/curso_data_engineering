{% test check_out_after_check_in(model, check_in_column, check_out_column) %}
    select *
    from {{ model }}
    where cast({{ check_out_column }} as timestamp) < cast({{ check_in_column }} as timestamp)
{% endtest %}