{% macro get_final_url_performance_columns() %}

{% set columns = [
    {"name": "_fivetran_id", "datatype": dbt_utils.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "account_descriptive_name", "datatype": dbt_utils.type_string(), "alias": "account_name"},
    {"name": "ad_group_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_group_name", "datatype": dbt_utils.type_string()},
    {"name": "ad_group_status", "datatype": dbt_utils.type_string()},
    {"name": "campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "campaign_name", "datatype": dbt_utils.type_string()},
    {"name": "campaign_status", "datatype": dbt_utils.type_string()},
    {"name": "clicks", "datatype": dbt_utils.type_int()},
    {"name": "cost", "datatype": dbt_utils.type_float(), "alias": "spend"},
    {"name": "date", "datatype": "date", "alias": "date_day"},
    {"name": "effective_final_url", "datatype": dbt_utils.type_string(), "alias": "final_url"},
    {"name": "external_customer_id", "datatype": dbt_utils.type_int()},
    {"name": "impressions", "datatype": dbt_utils.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_click_performance_columns() %}

{% set columns = [
    {"name": "_fivetran_id", "datatype": dbt_utils.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "account_descriptive_name", "datatype": dbt_utils.type_string(), "alias": "account_name"},
    {"name": "ad_group_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_group_name", "datatype": dbt_utils.type_string()},
    {"name": "ad_group_status", "datatype": dbt_utils.type_string()},
    {"name": "campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "campaign_name", "datatype": dbt_utils.type_string()},
    {"name": "campaign_status", "datatype": dbt_utils.type_string()},
    {"name": "clicks", "datatype": dbt_utils.type_int()},
    {"name": "criteria_id", "datatype": dbt_utils.type_int()},
    {"name": "date", "datatype": "date", "alias": "date_day"},
    {"name": "external_customer_id", "datatype": dbt_utils.type_int()},
    {"name": "gcl_id", "datatype": dbt_utils.type_string(), "alias": "gclid"}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_criteria_performance_columns() %}

{% set columns = [
    {"name": "_fivetran_id", "datatype": dbt_utils.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "account_descriptive_name", "datatype": dbt_utils.type_string(), "alias": "account_name"},
    {"name": "ad_group_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_group_name", "datatype": dbt_utils.type_string()},
    {"name": "ad_group_status", "datatype": dbt_utils.type_string()},
    {"name": "campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "campaign_name", "datatype": dbt_utils.type_string()},
    {"name": "campaign_status", "datatype": dbt_utils.type_string()},
    {"name": "clicks", "datatype": dbt_utils.type_int()},
    {"name": "cost", "datatype": dbt_utils.type_float(), "alias": "spend"},
    {"name": "criteria", "datatype": dbt_utils.type_string()},
    {"name": "criteria_destination_url", "datatype": dbt_utils.type_string()},
    {"name": "criteria_type", "datatype": dbt_utils.type_string()},
    {"name": "date", "datatype": "date", "alias": "date_day"},
    {"name": "external_customer_id", "datatype": dbt_utils.type_int()},
    {"name": "id", "datatype": dbt_utils.type_int()},
    {"name": "impressions", "datatype": dbt_utils.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}
