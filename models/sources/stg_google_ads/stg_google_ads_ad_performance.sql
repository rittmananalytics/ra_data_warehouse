{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}

{% if var("marketing_warehouse_ad_performance_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_performance_sources") %}

{% if var("stg_google_ads_etl") == 'segment' %}

with source as (
  {{ filter_segment_relation('segment_google_ads', 'ad_performance_reports') }}
),
renamed as (
SELECT
    date_start                    as ad_serve_ts,
    cast (ad_id as {{ dbt_utils.type_string() }})                      as ad_id,
    average_cost/1000000          AS ad_avg_cost,
    average_time_on_site          as ad_avg_time_on_site,
    bounce_rate                   as ad_bounce_rate,
    click_assisted_conversions    as ad_total_assisted_conversions,
    clicks                        as ad_total_clicks,
    conversion_value              as ad_total_conversion_value,
    cost/1000000                  as ad_total_cost,
    cast(null as {{ dbt_utils.type_int() }})                  as ad_total_impressions,
    cast(null as {{ dbt_utils.type_int() }})                        as ad_total_reach,
    cast(null as {{ dbt_utils.type_int() }})                as as_total_unique_clicks,
    cast(null as {{ dbt_utils.type_int() }})           as ad_total_unique_impressions,
    'Google Ads' as ad_network
FROM
  source)

{% elif var("stg_google_ads_etl") == 'stitch' %}

WITH source AS (
  {{ filter_stitch_relation(relation=source('stitch_google_ads', 'AD_PERFORMANCE_REPORT'),unique_column='concat(adid,day)') }}
),
renamed as (
SELECT
    day                    as ad_serve_ts,
    cast (adid as {{ dbt_utils.type_string() }})                  as ad_id,
    averagecost/1000000          AS ad_avg_cost,
    averagesessiondurationseconds          as ad_avg_time_on_site,
    bouncerate                   as ad_bounce_rate,
    clickassistedconv            as ad_total_assisted_conversions,
    clicks                       as ad_total_clicks,
    valueconv                    as ad_total_conversion_value,
    cost/1000000                 as ad_total_cost,
    cast(null as {{ dbt_utils.type_int() }})                  as ad_total_impressions,
    cast(null as {{ dbt_utils.type_int() }})                        as ad_total_reach,
    cast(null as {{ dbt_utils.type_int() }})                as as_total_unique_clicks,
    cast(null as {{ dbt_utils.type_int() }})           as ad_total_unique_impressions,
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
  {% else %} {{config(enabled=false)}} {% endif %}
