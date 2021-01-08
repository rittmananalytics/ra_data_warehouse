{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_google_ads_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_ad_performance_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_performance_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_google_ads_stitch_ad_performance_table'),unique_column='id') }}
),
renamed as (
SELECT
    day                    as ad_serve_ts,
    cast(ad_id as string)                  as ad_id,
    averagecost/1000000          AS ad_avg_cost,
    averagesessiondurationseconds          as ad_avg_time_on_site,
    bouncerate                   as ad_bounce_rate,
    clickassistedconv            as ad_total_assisted_conversions,
    clicks                       as ad_total_clicks,
    valueconv                    as ad_total_conversion_value,
    cost/1000000                 as ad_total_cost,
    cast(null as int64)                  as ad_total_impressions,
    cast(null as int64)                        as ad_total_reach,
    cast(null as int64)                as as_total_unique_clicks,
    cast(null as int64)           as ad_total_unique_impressions,
    'Google Ads' as ad_network
FROM
  source)
select
  *
from
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
