{%- macro cast(value,datatype) -%}

{%- if datatype|default('string') == 'string' %}

{% if target.type == 'bigquery' or target.type == 'snowflake' %}
  cast({{ value |default("null")}}  as string )
{% elif target.type == 'redshift' %}
  cast({{ value|default("null")}} as varchar )
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% elif datatype == 'integer' %}

{% if target.type == 'bigquery'  %}
  cast({{ value|default("null")}} as int64 )
{% elif target.type == 'redshift' or target.type == 'snowflake' %}
  cast({{ value|default("null")}} as int )
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% elif datatype == 'float' %}

{% if target.type == 'bigquery'  %}
  cast({{ value|default("null")}} as float64 )
{% elif target.type == 'redshift' or target.type == 'snowflake' %}
  cast({{ value|default("null")}} as float )
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% elif datatype == 'timestamp' %}

{% if target.type == 'bigquery' or target.type == 'redshift' or target.type == 'snowflake' %}
  cast({{ value|default("null")}} as timestamp )
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{%- endmacro -%}
