{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_google_ads_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_ad_campaign_performance_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_campaign_performance_sources") %}

WITH source AS (
  select * from (
    SELECT *,
    max(_sdc_report_datetime) over (partition by campaignid, day) as max_sdc_report_datetime
    FROM {{ var('stg_google_ads_stitch_campaign_performance_table') }})
where _sdc_report_datetime = max_sdc_report_datetime
order by campaignid, day
),
renamed as (
SELECT
  day                           as ad_campaign_serve_ts,
  cast(campaignid  as string)                  as ad_campaign_id,
  budget/1000000                AS ad_campaign_budget,
  avgcost/1000000           AS ad_campaign_avg_cost,
  avgsessiondurationseconds as ad_campaign_avg_time_on_site,
  bouncerate                    as ad_campaign_bounce_rate,
  cast(null as string)          as ad_campaign_status,
  clickassistedconv             as ad_campaign_total_assisted_conversions,
  clicks                        as ad_campaign_total_clicks,
  valueconv                     as ad_campaign_total_conversion_value,
  conversions                   as ad_campaign_total_conversions,
  cost/1000000       as ad_campaign_total_cost,
  engagements                   as ad_campaign_total_engagements,
  impressions                   as ad_campaign_total_impressions,
  invalidclicks                 as ad_campaign_total_invalid_clicks,
  'Google Ads' as ad_network
FROM
  source)
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
