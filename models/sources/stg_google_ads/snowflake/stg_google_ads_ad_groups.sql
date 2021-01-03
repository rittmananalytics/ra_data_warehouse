{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_ad_group_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_group_sources") %}

{% if var("stg_google_ads_etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_google_ads_stitch_ad_groups_table'),unique_column='id') }}
),
renamed as (

  SELECT cast(id as string) as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
         cast(campaign_id as string) ad_campaign_id,
         'Google Ads' as ad_network
  FROM source

)
{% elif var("stg_google_ads_etl") == 'segment' %}
with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_ad_groups_table')) }}
),
renamed as (
  SELECT cast(id as string) as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
         cast(campaign_id as string) ad_campaign_id,
         'Google Ads' as ad_network
  FROM source )
{% endif %}
select
 *
from
 renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
