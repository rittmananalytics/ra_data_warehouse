{% macro get_fill_staging_columns_columns() %}

{% set columns = [
    {"name": "column_date", "datatype": "date"},
    {"name": "column_string", "datatype": dbt_utils.type_string()},
    {"name": "column_int", "datatype": dbt_utils.type_int(), "alias": "column_int_alias"},
    {"name": "column_float", "datatype": dbt_utils.type_float()}
] %}

{{ return(columns) }}

{% endmacro %}