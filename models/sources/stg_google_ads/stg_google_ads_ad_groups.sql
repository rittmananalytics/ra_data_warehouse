{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}

{% if var("marketing_warehouse_ad_group_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_group_sources") %}

{% if var("stg_google_ads_etl") == 'segment' %}

with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_ad_groups_table')) }}
),
renamed as (
  SELECT {{ cast('id','string') }} as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
          {{ cast('campaign_id','string') }} ad_campaign_id,
         'Google Ads' as ad_network
  FROM source )

{% elif var("stg_google_ads_etl") == 'stitch' %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_google_ads_stitch_ad_groups_table'),unique_column='id') }}
),
renamed as (

  SELECT {{ cast('id','string') }} as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
          {{ cast('campaignid','string') }} ad_campaign_id,
          {{ cast(datatype='timestamp')}} as adset_created_ts,
          {{ cast(datatype='timestamp')}} as adset_end_ts,
          {{ cast(datatype='timestamp')}} as adset_start_ts,
         'Google Ads' as ad_network
  FROM source
)
{% endif %}

select
 *
from
 renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
