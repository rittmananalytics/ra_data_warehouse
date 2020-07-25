{% if not var("enable_google_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_table(var('stitch_schema'),var('stitch_ad_performance_table'),'id') }}
),
renamed as (
SELECT
    day                    as ad_serve_ts,
    concat('{{ var('id-prefix') }}',ad_id)                   as ad_id,
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
{% elif var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_ad_performance_table')) }}
),
renamed as (
SELECT
    date_start                    as ad_serve_ts,
    concat('{{ var('id-prefix') }}',ad_id)                   as ad_id,
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
{% endif %}
select
  *
from
  renamed
