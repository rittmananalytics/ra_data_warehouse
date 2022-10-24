{% macro pivot_json_extract(string, list_of_properties) %}

{%- for property in list_of_properties -%}

replace( {{ fivetran_utils.json_extract(string, property) }}, '"', '') as {{ property | replace(' ', '_') | lower }}

{%- if not loop.last -%},{%- endif %}
{% endfor -%}

{% endmacro %}