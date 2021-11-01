{% macro get_google_ads_ad_group_history_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "ad_group_rotation_mode", "datatype": dbt_utils.type_string()},
    {"name": "ad_group_type", "datatype": dbt_utils.type_string()},
    {"name": "base_ad_group_id", "datatype": dbt_utils.type_int()},
    {"name": "base_campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_bid_ceiling", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_bid_changes_for_raises_only", "datatype": "boolean"},
    {"name": "bidding_strategy_bid_floor", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_bid_modifier", "datatype": dbt_utils.type_float()},
    {"name": "bidding_strategy_competitor_domain", "datatype": dbt_utils.type_string()},
    {"name": "bidding_strategy_cpa_bid_amount", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_cpc_bid_amount", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_cpm_bid_amount", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_enhanced_cpc_enabled", "datatype": "boolean"},
    {"name": "bidding_strategy_id", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_max_cpc_bid_ceiling", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_max_cpc_bid_floor", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_name", "datatype": dbt_utils.type_string()},
    {"name": "bidding_strategy_raise_bid_when_budget_constrained", "datatype": "boolean"},
    {"name": "bidding_strategy_raise_bid_when_low_quality_score", "datatype": "boolean"},
    {"name": "bidding_strategy_scheme_type", "datatype": dbt_utils.type_string()},
    {"name": "bidding_strategy_source", "datatype": dbt_utils.type_string()},
    {"name": "bidding_strategy_spend_target", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_strategy_goal", "datatype": dbt_utils.type_string()},
    {"name": "bidding_strategy_target_cpa", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_target_outrank_share", "datatype": dbt_utils.type_int()},
    {"name": "bidding_strategy_target_roas", "datatype": dbt_utils.type_float()},
    {"name": "bidding_strategy_target_roas_override", "datatype": dbt_utils.type_float()},
    {"name": "bidding_strategy_type", "datatype": dbt_utils.type_string()},
    {"name": "bidding_strategy_viewable_cpm_enabled", "datatype": "boolean"},
    {"name": "campaign_id", "datatype": dbt_utils.type_int()},
    {"name": "campaign_name", "datatype": dbt_utils.type_string()},
    {"name": "content_bid_criterion_type_group", "datatype": dbt_utils.type_string()},
    {"name": "final_url_suffix", "datatype": dbt_utils.type_string()},
    {"name": "id", "datatype": dbt_utils.type_int()},
    {"name": "name", "datatype": dbt_utils.type_string()},
    {"name": "status", "datatype": dbt_utils.type_string()},
    {"name": "tracking_url_template", "datatype": dbt_utils.type_string()},
    {"name": "updated_at", "datatype": dbt_utils.type_timestamp()}
] %}

{{ return(columns) }}

{% endmacro %}
