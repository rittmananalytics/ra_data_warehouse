{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_google_ads_etl") == 'segment')
   )
}}
{% if var("marketing_warehouse_ad_campaign_performance_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_campaign_performance_sources") %}

with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_ad_performance_table')) }}
),
renamed as (
SELECT
  date_start                    as ad_campaign_serve_ts,
  cast(campaign_id as string)                  as ad_campaign_id,
  amount/1000000                AS ad_campaign_budget,
  average_cost/1000000          AS ad_campaign_avg_cost,
  average_time_on_site          as ad_campaign_avg_time_on_site,
  bounce_rate                   as ad_campaign_bounce_rate,
  campaign_status               as ad_campaign_status,
  click_assisted_conversions    as ad_campaign_total_assisted_conversions,
  clicks                        as ad_campaign_total_clicks,
  conversion_value              as ad_campaign_total_conversion_value,
  conversions                   as ad_campaign_total_conversions,
  cost/1000000       as ad_campaign_total_cost,
  engagements                   as ad_campaign_total_engagements,
  impressions                   as ad_campaign_total_impressions,
  invalid_clicks                as ad_campaign_total_invalid_clicks,
  'Google Ads' as ad_network
FROM
  source)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
