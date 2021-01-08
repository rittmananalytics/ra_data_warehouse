{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_google_ads_etl") == 'segment')
   )
}}
{% if var("marketing_warehouse_ad_performance_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_performance_sources") %}

with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_ad_performance_table')) }}
),
renamed as (
SELECT
    date_start                    as ad_serve_ts,
    cast(ad_id as string)                      as ad_id,
    average_cost/1000000          AS ad_avg_cost,
    average_time_on_site          as ad_avg_time_on_site,
    bounce_rate                   as ad_bounce_rate,
    click_assisted_conversions    as ad_total_assisted_conversions,
    clicks                        as ad_total_clicks,
    conversion_value              as ad_total_conversion_value,
    cost/1000000                  as ad_total_cost,
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
