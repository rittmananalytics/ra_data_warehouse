{% macro get_facebook_ad_history_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "account_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_set_id", "datatype": dbt_utils.type_int()},
    {"name": "ad_source_id", "datatype": dbt_utils.type_int()},
    {"name": "bid_amount", "datatype": dbt_utils.type_int()},
    {"name": "bid_info_actions", "datatype": dbt_utils.type_int()},
    {"name": "bid_type", "datatype": dbt_utils.type_string()},
    {"name": "campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "configured_status", "datatype": dbt_utils.type_string()},
    {"name": "created_time", "datatype": dbt_utils.type_timestamp()},
    {"name": "creative_id", "datatype": dbt_utils.type_int()},
    {"name": "effective_status", "datatype": dbt_utils.type_string()},
    {"name": "global_discriminatory_practices", "datatype": dbt_utils.type_string()},
    {"name": "global_non_functional_landing_page", "datatype": dbt_utils.type_string()},
    {"name": "global_use_of_our_brand_assets", "datatype": dbt_utils.type_string()},
    {"name": "id", "datatype": dbt_utils.type_int()},
    {"name": "last_updated_by_app_id", "datatype": dbt_utils.type_string()},
    {"name": "name", "datatype": dbt_utils.type_string()},
    {"name": "placement_specific_facebook_discriminatory_practices", "datatype": dbt_utils.type_string()},
    {"name": "placement_specific_facebook_non_functional_landing_page", "datatype": dbt_utils.type_string()},
    {"name": "placement_specific_facebook_use_of_our_brand_assets", "datatype": dbt_utils.type_string()},
    {"name": "placement_specific_instagram_discriminatory_practices", "datatype": dbt_utils.type_string()},
    {"name": "status", "datatype": dbt_utils.type_string()},
    {"name": "updated_time", "datatype": dbt_utils.type_timestamp()}
] %}

{{ return(columns) }}

{% endmacro %}
