{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_ad_campaign_performance_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_campaign_performance_sources") %}

{% if var("stg_google_ads_etl") == 'stitch' %}
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
  {{ dbt_utils.date_trunc('DAY','day') }}      as ad_campaign_serve_ts,
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
{% elif var("stg_google_ads_etl") == 'segment' %}
with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_campaign_performance_table')) }}
),
renamed as (
SELECT
  {{ dbt_utils.date_trunc('DAY','date_start') }}                    as ad_campaign_serve_ts,
  cast(campaign_id as string)   as ad_campaign_id,
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
{% endif %}
select
  *
from
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
