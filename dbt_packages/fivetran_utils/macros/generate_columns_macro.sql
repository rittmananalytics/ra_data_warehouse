{% macro generate_columns_macro(table_name, schema_name, database_name=target.database) %}

{% set columns = get_columns_for_macro(table_name, schema_name, database_name) %}

{% set jinja_macro=[] %}

{% do jinja_macro.append('{% macro get_' ~ table_name ~ '_columns() %}') %}
{% do jinja_macro.append('') %}
{% do jinja_macro.append('{% set columns = [') %}

{% for col in columns %}
{% do jinja_macro.append('    ' ~ col ~ (',' if not loop.last)) %}
{% endfor %}

{% do jinja_macro.append('] %}') %}
{% do jinja_macro.append('') %}
{% do jinja_macro.append('{{ return(columns) }}') %}
{% do jinja_macro.append('') %}
{% do jinja_macro.append('{% endmacro %}') %}

{% if execute %}

    {% set joined = jinja_macro | join ('\n') %}
    {{ log(joined, info=True) }}
    {% do return(joined) %}

{% endif %}

{% endmacro %}