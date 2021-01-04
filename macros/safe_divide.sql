{% macro safe_divide(dividend,divisor) %}

{{ dividend }}/nullif({{ divisor }},0)

{% endmacro %}
