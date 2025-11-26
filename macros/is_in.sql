{% macro is_in(column, values) %}
    case
        when {{ column }} in (
            {% for v in values %}
                '{{ v }}'{% if not loop.last %},{% endif %}
            {% endfor %}
        )
        then true
        else false
    end
{% endmacro %}