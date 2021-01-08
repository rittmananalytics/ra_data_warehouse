{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_facebook_ads_etl") == 'segment')
   )
}}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}


with source as (
  {{ filter_segment_relation(var('stg_facebook_ads_segment_ads_table')) }}
),
renamed as (
SELECT
      cast(id as string)           as ad_id,
      status      as ad_status,
      cast(null as string)        as ad_type,
      cast(null as string)   as ad_final_urls,
      cast(adset_id as string) as ad_group_id,
      bid_type as ad_bid_type,
      url_parameters as ad_utm_parameters,
      utm_campaign as ad_utm_campaign,
      utm_content as ad_utm_content,
      utm_medium as ad_utm_medium,
      utm_source as ad_utm_source,
      'Facebook Ads' as ad_network
FROM
  source)
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
