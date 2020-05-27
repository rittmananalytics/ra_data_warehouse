{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('ads_insights_table'),'ad_id') }}

),
renamed as (
  SELECT
  concat('{{ var('id-prefix') }}',ad_id) as ad_id,
  current_timestamp as ad_snapshot_ts,
  concat('{{ var('id-prefix') }}',adset_id) as adset_id,
  concat('{{ var('id-prefix') }}',campaign_id) as campaign_id,
  concat('{{ var('id-prefix') }}',account_id) as account_id,
  unique_actions.action_type as ad_action_type,
  unique_actions._1d_click as ad_total_1d_clicks,
  unique_actions._7d_click as ad_total_7d_clicks,
  unique_actions._28d_click as ad_total_28d_clicks,
  unique_actions.value as ad_action_value
FROM
  `ra-development.stitch_facebook_ads.ads_insights`,
  unnest(unique_actions) unique_actions
    )
select * from renamed
