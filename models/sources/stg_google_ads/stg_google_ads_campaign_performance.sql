{% if not var("enable_google_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_table(var('stitch_schema'),var('stitch_campaign_performance_table'),'id') }}
),
renamed as (
SELECT
  day                           as ad_campaign_serve_ts,
  concat('{{ var('id-prefix') }}',campaignid)                    as ad_campaign_id,
  amount/1000000                AS ad_campaign_budget,
  averagecost/1000000           AS ad_campaign_avg_cost,
  averagesessiondurationseconds as ad_campaign_avg_time_on_site,
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
{% elif var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_campaign_performance_table')) }}
),
renamed as (
SELECT
  date_start                    as ad_campaign_serve_ts,
  concat('{{ var('id-prefix') }}',campaign_id)                   as ad_campaign_id,
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
