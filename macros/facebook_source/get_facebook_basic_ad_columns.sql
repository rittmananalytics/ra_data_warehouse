{% macro get_facebook_basic_ad_columns() %}

{% set columns = [
    {"name": "_fivetran_id", "datatype": dbt_utils.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "account_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_id", "datatype": dbt_utils.type_string()},
    {"name": "ad_name", "datatype": dbt_utils.type_string()},
    {"name": "adset_name", "datatype": dbt_utils.type_string()},
    {"name": "cpc", "datatype": dbt_utils.type_float()},
    {"name": "cpm", "datatype": dbt_utils.type_float()},
    {"name": "ctr", "datatype": dbt_utils.type_float()},
    {"name": "date", "datatype": "date"},
    {"name": "frequency", "datatype": dbt_utils.type_float()},
    {"name": "impressions", "datatype": dbt_utils.type_int()},
    {"name": "inline_link_clicks", "datatype": dbt_utils.type_int()},
    {"name": "reach", "datatype": dbt_utils.type_int()},
    {"name": "spend", "datatype": dbt_utils.type_float()}
] %}

{{ return(columns) }}

{% endmacro %}
