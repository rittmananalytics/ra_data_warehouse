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
    concat('facebook-ads-',ad_id) as ad_id,
    current_timestamp as ad_snapshot_ts,
    concat('facebook-ads-',adset_id) as adset_id,
    concat('facebook-ads-',campaign_id) as campaign_id,
    concat('facebook-ads-',account_id) as account_id,
    date_start,
    date_stop,
    impressions,
    clicks,
    reach,
    inline_link_clicks,
    unique_clicks,
    objective,
    unique_inline_link_clicks,
    spend,
    social_spend,
    inline_post_engagement,
    frequency
  FROM
    source
    )
select * from renamed
