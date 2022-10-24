{% macro default__get_columns_for_macro(table_name, schema_name, database_name=target.database) %}

{% set query %}

select
    concat(
      '{"name": "', 
      lower(column_name), 
      '", "datatype": ',
      case
        when lower(data_type) like '%timestamp%' then 'dbt_utils.type_timestamp()' 
        when lower(data_type) = 'text' then 'dbt_utils.type_string()' 
        when lower(data_type) = 'boolean' then '"boolean"'
        when lower(data_type) = 'number' then 'dbt_utils.type_numeric()' 
        when lower(data_type) = 'float' then 'dbt_utils.type_float()' 
        when lower(data_type) = 'date' then '"date"'
      end,
      '}')
from {{ database_name }}.information_schema.columns
where lower(table_name) = '{{ table_name }}'
and lower(table_schema) = '{{ schema_name }}'
order by 1

{% endset %}

{% set results = run_query(query) %}
{% set results_list = results.columns[0].values() %}}

{{ return(results_list) }}

{% endmacro %}



{% macro bigquery__get_columns_for_macro(table_name, schema_name, database_name=target.database) %}

{% set query %}

select
    concat(
      '{"name": "', 
      lower(column_name), 
      '", "datatype": ',
      case
        when lower(data_type) like '%timestamp%' then 'dbt_utils.type_timestamp()' 
        when lower(data_type) = 'string' then 'dbt_utils.type_string()' 
        when lower(data_type) = 'bool' then '"boolean"'
        when lower(data_type) = 'numeric' then 'dbt_utils.type_numeric()' 
        when lower(data_type) = 'float64' then 'dbt_utils.type_float()' 
        when lower(data_type) = 'int64' then 'dbt_utils.type_int()' 
        when lower(data_type) = 'date' then '"date"' 
        when lower(data_type) = 'datetime' then '"datetime"' 
      end,
      '}')
from `{{ database_name }}`.{{ schema_name }}.INFORMATION_SCHEMA.COLUMNS
where lower(table_name) = '{{ table_name }}'
and lower(table_schema) = '{{ schema_name }}'
order by 1

{% endset %}

{% set results = run_query(query) %}
{% set results_list = results.columns[0].values() %}}

{{ return(results_list) }}

{% endmacro %}



{% macro get_columns_for_macro(table_name, schema_name, database_name) -%}
  {{ return(adapter.dispatch('get_columns_for_macro')(table_name, schema_name, database_name)) }}
{%- endmacro %}