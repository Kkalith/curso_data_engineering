{% test is_email(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and not {{ column_name }} ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
{% endtest %}