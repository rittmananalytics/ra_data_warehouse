{% macro persist_pass_through_columns(pass_through_variable) %}

{% if var(pass_through_variable, none) %}
    {% for field in var(pass_through_variable) %}
        , {{ field.alias if field.alias else field.name }}
    {% endfor %}
{% endif %}

{% endmacro %}