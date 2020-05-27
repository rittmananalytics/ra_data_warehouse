{%- macro bootstrap_schemas(staging_schema_name, seed_schema_name, reset) -%}

{%- set schema_prefix = env_var('schema_prefix','') -%}

{% if schema_prefix|length %}
{%- set schema_prefix = schema_prefix~"_" -%}
{% endif %}

{% if reset %}

{{ drop_schema(target.database, schema_prefix~target.schema) }}
{{ drop_schema(target.database, schema_prefix~target.schema~"_"~staging_schema_name) }}
{{ drop_schema(target.database, schema_prefix~target.schema~"_logs") }}
{{ drop_schema(target.database, schema_prefix~target.schema~"_"~seed_schema_name) }}

{% endif %}

{{ create_schema(target.database, schema_prefix~target.schema) }}
{{ create_schema(target.database, schema_prefix~target.schema~"_"~staging_schema_name) }}
{{ create_schema(target.database, schema_prefix~target.schema~"_logs") }}
{{ create_schema(target.database, schema_prefix~target.schema~"_"~seed_schema_name) }}

{%- endmacro -%}

{%- macro drop_schemas(staging_schema_name, seed_schema_name, logs_schema_name) -%}

{{ drop_schema(target.database, schema_prefix~target.schema) }}
{{ drop_schema(target.database, schema_prefix~target.schema~"_"~staging_schema_name) }}
{{ drop_schema(target.database, schema_prefix~target.schema~"_logs") }}
{{ drop_schema(target.database, schema_prefix~target.schema~"_"~seed_schema_name) }}

{%- endmacro -%}
