{% macro get_facebook_campaign_history_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "account_id", "datatype": dbt_utils.type_int()},
    {"name": "boosted_object_id", "datatype": dbt_utils.type_int()},
    {"name": "budget_rebalance_flag", "datatype": "boolean"},
    {"name": "buying_type", "datatype": dbt_utils.type_string()},
    {"name": "can_create_brand_lift_study", "datatype": "boolean"},
    {"name": "can_use_spend_cap", "datatype": "boolean"},
    {"name": "configured_status", "datatype": dbt_utils.type_string()},
    {"name": "created_time", "datatype": dbt_utils.type_timestamp()},
    {"name": "daily_budget", "datatype": dbt_utils.type_int()},
    {"name": "effective_status", "datatype": dbt_utils.type_string()},
    {"name": "id", "datatype": dbt_utils.type_int()},
    {"name": "name", "datatype": dbt_utils.type_string()},
    {"name": "objective", "datatype": dbt_utils.type_string()},
    {"name": "source_campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "spend_cap", "datatype": dbt_utils.type_int()},
    {"name": "start_time", "datatype": dbt_utils.type_timestamp()},
    {"name": "status", "datatype": dbt_utils.type_string()},
    {"name": "stop_time", "datatype": dbt_utils.type_timestamp()},
    {"name": "updated_time", "datatype": dbt_utils.type_timestamp()}
] %}

{{ return(columns) }}

{% endmacro %}
