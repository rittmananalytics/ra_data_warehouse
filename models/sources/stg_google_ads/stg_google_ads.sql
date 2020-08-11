{% if not var("enable_google_ads_source") or not var("ad_campaigns_only") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% if var("stg_google_ads_etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_table(var('stg_google_ads_stitch_schema'),var('stg_google_ads_stitch_ads_table'),'id') }}
),
renamed as (

    select
    concat('{{ var('stg_google_ads_id-prefix') }}',id)              as ad_id,
    status      as ad_status,
    type        as ad_type,
    finalurls  as ad_final_urls,
    concat('{{ var('stg_google_ads_id-prefix') }}',adgroupid) as ad_group_id,
    cast(null as string) as ad_bid_type,
    cast(null as string)  as ad_utm_parameters,
    cast(null as string)  as ad_utm_campaign,
    cast(null as string)  as ad_utm_content,
    cast(null as string)  as ad_utm_medium,
    cast(null as string)  as ad_utm_source,
    'Google Ads' as ad_network

    from source
)
{% elif var("stg_google_ads_etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('stg_google_ads_segment_schema'),var('stg_google_ads_segment_ads_table')) }}
),
renamed as (
SELECT
      concat('{{ var('stg_google_ads_id-prefix') }}',id)          as ad_id,
      status      as ad_status,
      type        as ad_type,
      final_urls  as ad_final_urls,
      concat('{{ var('stg_google_ads_id-prefix') }}',ad_group_id) as ad_group_id,
      cast(null as string) as ad_bid_type,
      cast(null as string)  as ad_utm_parameters,
      cast(null as string)  as ad_utm_campaign,
      cast(null as string)  as ad_utm_content,
      cast(null as string)  as ad_utm_medium,
      cast(null as string)  as ad_utm_source,
      'Google Ads' as ad_network
FROM
  source)
{% endif %}
select
 *
from
 renamed
