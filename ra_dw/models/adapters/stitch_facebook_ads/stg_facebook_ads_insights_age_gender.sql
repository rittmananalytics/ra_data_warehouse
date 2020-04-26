{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
        SELECT
        *
      FROM
        {{ source('stitch_facebook_ads', 's_ads_insights_age_and_gender') }}
    ),
renamed as (
  SELECT
   adset_id,
   adset_name,
   ad_id,
   ad_name,
   account_id,
   account_name,
   campaign_id,
   campaign_name,
   ctr,
   unique_ctr,
   date_start,
   date_stop,
   gender,
   age,
   impressions,
   clicks,
   objective,
   unique_clicks,
   inline_link_clicks,
   unique_inline_link_clicks,
   inline_post_engagement,
   frequency,
   unique_link_clicks_ctr,
   inline_link_click_ctr,
   cpp,
   cpc,
   spend,
   cost_per_inline_link_click,
   cost_per_unique_inline_link_click,
   cost_per_unique_click,
   cost_per_inline_post_engagement
 FROM
   source
   {{ dbt_utils.group_by(n=31) }}
)
select * from renamed
