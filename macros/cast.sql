{%- macro CAST(value,datatype) -%}

{%- if datatype|default('string') == 'string' %}

{% if target.type == 'bigquery' or target.type == 'snowflake' %}
  CAST({{ value |default("null")}}  AS string )
{% elif target.type == 'redshift' %}
  CAST({{ value|default("null")}} AS varchar )
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% elif datatype == 'integer' %}

{% if target.type == 'bigquery'  %}
  CAST({{ value|default("null")}} AS int64 )
{% elif target.type == 'redshift' or target.type == 'snowflake' %}
  CAST({{ value|default("null")}} AS int )
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% elif datatype == 'float' %}

{% if target.type == 'bigquery'  %}
  CAST({{ value|default("null")}} AS float64 )
{% elif target.type == 'redshift' or target.type == 'snowflake' %}
  CAST({{ value|default("null")}} AS float )
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% elif datatype == 'timestamp' %}

{% if target.type == 'bigquery' or target.type == 'redshift' or target.type == 'snowflake' %}
  CAST({{ value|default("null")}} AS timestamp )
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{%- endmacro -%}
