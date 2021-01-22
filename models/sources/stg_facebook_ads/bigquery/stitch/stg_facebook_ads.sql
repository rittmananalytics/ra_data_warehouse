{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_facebook_ads_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_facebook_ads_stitch_ads_table'),unique_column='id') }}
),
renamed as (

    select
    cast(id as string)              as ad_id,
    status      as ad_status,
    cast(null as string)        as ad_type,
    cast(null as string)   as ad_final_urls,
    cast(adset_id as string) as ad_group_id,
    bid_type as ad_bid_type,
    cast(null as string) as ad_utm_parameters,
    cast(null as string) as ad_utm_campaign,
    cast(null as string)as ad_utm_content,
    cast(null as string)as ad_utm_medium,
    cast(null as string) as ad_utm_source,
    'Facebook Ads' as ad_network

    from source
)
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
