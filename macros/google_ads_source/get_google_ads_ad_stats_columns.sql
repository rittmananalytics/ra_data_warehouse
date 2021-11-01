{% macro get_google_ads_ad_stats_columns() %}

{% set columns = [
    {"name": "_fivetran_id", "datatype": dbt_utils.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "active_view_impressions", "datatype": dbt_utils.type_int()},
    {"name": "active_view_measurability", "datatype": dbt_utils.type_float()},
    {"name": "active_view_measurable_cost_micros", "datatype": dbt_utils.type_int()},
    {"name": "active_view_measurable_impressions", "datatype": dbt_utils.type_int()},
    {"name": "active_view_viewability", "datatype": dbt_utils.type_float()},
    {"name": "ad_group", "datatype": dbt_utils.type_string()},
    {"name": "ad_group_base_ad_group", "datatype": dbt_utils.type_string()},
    {"name": "ad_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_network_type", "datatype": dbt_utils.type_string()},
    {"name": "campaign_base_campaign", "datatype": dbt_utils.type_string()},
    {"name": "campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "clicks", "datatype": dbt_utils.type_int()},
    {"name": "conversions", "datatype": dbt_utils.type_float()},
    {"name": "conversions_value", "datatype": dbt_utils.type_float()},
    {"name": "cost_micros", "datatype": dbt_utils.type_int()},
    {"name": "customer_id", "datatype": dbt_utils.type_int()},
    {"name": "date", "datatype": "date"},
    {"name": "device", "datatype": dbt_utils.type_string()},
    {"name": "impressions", "datatype": dbt_utils.type_int()},
    {"name": "interaction_event_types", "datatype": dbt_utils.type_string()},
    {"name": "interactions", "datatype": dbt_utils.type_int()},
    {"name": "keyword_ad_group_criterion", "datatype": dbt_utils.type_string()},
    {"name": "view_through_conversions", "datatype": dbt_utils.type_int()}
] %}

{{ return(columns) }}

{% endmacro %}
