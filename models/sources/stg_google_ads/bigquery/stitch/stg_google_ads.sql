{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_google_ads_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_google_ads_stitch_ads_table'),unique_column='id') }}
),
renamed as (

    select
    cast(id as string)             as ad_id,
    status      as ad_status,
    type        as ad_type,
    finalurls  as ad_final_urls,
    cast(adgroupid as string) as ad_group_id,
    cast(null as string) as ad_bid_type,
    cast(null as string)  as ad_utm_parameters,
    cast(null as string)  as ad_utm_campaign,
    cast(null as string)  as ad_utm_content,
    cast(null as string)  as ad_utm_medium,
    cast(null as string)  as ad_utm_source,
    'Google Ads' as ad_network

    from source
)
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
