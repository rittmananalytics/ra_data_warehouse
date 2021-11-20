{% macro get_tables(schema, prefix='', exclude='') %}

    {%- call statement('tables', fetch_result=True) %}

        SELECT
            distinct table_schema || '.' || table_name AS ref
        FROM {{ schema }}.INFORMATION_SCHEMA.TABLES
        where table_schema = '{{ schema }}'

    {%- endcall -%}

    {%- set table_list = load_result('tables') -%}

    {%- if table_list and table_list['data'] -%}
        {%- set tables = table_list['data'] | map(attribute=0) | list %}
        {{ return(tables) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}
