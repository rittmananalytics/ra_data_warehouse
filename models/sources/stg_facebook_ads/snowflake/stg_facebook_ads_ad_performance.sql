{{config(enabled = target.type == 'snowflake')}}

{% if var("marketing_warehouse_ad_performance_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_performance_sources") %}

{% if var("stg_facebook_ads_etl") == 'stitch' %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_facebook_ads_stitch_ad_performance_table_snowflake'),unique_column='concat(ad_id,date_start::STRING)') }}
),
renamed as (
SELECT
    date_start                              as ad_serve_ts,
    cast(ad_id as string)                   as ad_id,
    {{safe_divide('spend','clicks')}}       as ad_avg_cost,
    cast(null as timestamp)                 as ad_avg_time_on_site,
    cast(null as float)                     as ad_bounce_rate,
    cast(null as int)                       as ad_total_assisted_conversions,
    clicks                                  as ad_total_clicks,
    impressions                  as ad_total_impressions,
    reach                        as ad_total_reach,
    unique_clicks                as as_total_unique_clicks,
    null           as ad_total_unique_impressions,
    cast(null as float)        as ad_total_conversion_value,
    spend                        as ad_total_cost,
    'Facebook Ads' as ad_network
FROM
  source)
{% elif var("stg_facebook_ads_etl") == 'segment' %}
with source as (
  {{ filter_segment_relation(relation=var('stg_facebook_ads_segment_ad_performance_table')) }}
),
renamed as (
SELECT
    date_start                    as ad_serve_ts,
    cast(ad_id as string)                   as ad_id,
    {{ safe_divide('spend','clicks') }}   AS ad_avg_cost,
    cast(null as float)      as ad_avg_time_on_site,
    cast(null as float)        as ad_bounce_rate,
    cast(null as int)          as ad_total_assisted_conversions,
    clicks                       as ad_total_clicks,
    impressions                  as ad_total_impressions,
    reach                        as ad_total_reach,
    unique_clicks                as as_total_unique_clicks,
    unique_impressions           as ad_total_unique_impressions,
    cast(null as float)        as ad_total_conversion_value,
    spend                        as ad_total_cost,
    'Facebook Ads' as ad_network
FROM
  source)
{% endif %}
select
  *
from
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
