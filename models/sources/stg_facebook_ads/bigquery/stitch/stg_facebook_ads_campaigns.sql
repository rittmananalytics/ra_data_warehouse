{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_facebook_ads_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_ad_campaign_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_campaign_sources") %}

with source as (

  {{ filter_stitch_relation(relation=var('stg_facebook_ads_stitch_campaigns_table'),unique_column='id') }}

),
renamed as (

    select
    cast(id as string)      as campaign_id,
    name      as campaign_name,
    status          as ad_campaign_status,
    effective_status as campaign_effective_status,
    start_time      as ad_campaign_start_date,
    stop_time        as ad_campaign_end_date,
    'Facebook Ads' as ad_network

    from source

)
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
