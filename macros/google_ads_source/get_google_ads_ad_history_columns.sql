{% macro get_google_ads_ad_history_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "ad_group_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_type", "datatype": dbt_utils.type_string()},
    {"name": "automated", "datatype": "boolean"},
    {"name": "base_adgroup_id", "datatype": dbt_utils.type_int()},
    {"name": "base_campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "device_preference", "datatype": dbt_utils.type_int()},
    {"name": "display_url", "datatype": dbt_utils.type_string()},
    {"name": "final_url_suffix", "datatype": dbt_utils.type_string()},
    {"name": "id", "datatype": dbt_utils.type_int()},
    {"name": "policy_summary_combined_approval_status", "datatype": dbt_utils.type_string()},
    {"name": "policy_summary_denormalized_status", "datatype": dbt_utils.type_string()},
    {"name": "policy_summary_review_state", "datatype": dbt_utils.type_string()},
    {"name": "status", "datatype": dbt_utils.type_string()},
    {"name": "system_managed_entity_source", "datatype": dbt_utils.type_string()},
    {"name": "tracking_url_template", "datatype": dbt_utils.type_string()},
    {"name": "updated_at", "datatype": dbt_utils.type_timestamp()},
    {"name": "url", "datatype": dbt_utils.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}
