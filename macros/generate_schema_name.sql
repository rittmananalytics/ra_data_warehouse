{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set schema_prefix = env_var('schema_prefix','') -%}

    {% if schema_prefix|length %}
    {%- set schema_prefix = schema_prefix~"_" -%}
    {% endif %}

    {%- if custom_schema_name is none -%}
        {{ schema_prefix }}{{ default_schema  }}
    {%- else -%}
        {{ schema_prefix }}{{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro generate_prefixed_target_name() -%}
{%- set default_schema = target.schema -%}
{%- set schema_prefix = env_var('schema_prefix','') -%}

{% if schema_prefix|length %}
{%- set schema_prefix = schema_prefix~"_" -%}
{% endif %}

{{ schema_prefix }}{{ default_schema  }}

{%- endmacro %}
