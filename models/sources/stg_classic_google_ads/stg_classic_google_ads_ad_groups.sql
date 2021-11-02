{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}

{% if var("marketing_warehouse_ad_group_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_group_sources") %}

{% if var("stg_google_ads_etl") == 'segment' %}

with source as (
  {{ filter_segment_relation(relation=source('segment_google_ads', 'ad_groups')) }}
),
renamed as (
  SELECT cast(id as {{ dbt_utils.type_string() }}) as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
          cast(campaign_id as {{ dbt_utils.type_string() }}) ad_campaign_id,
         'Google Ads' as ad_network
  FROM source )

{% elif var("stg_google_ads_etl") == 'stitch' %}

WITH source AS (
  {{ filter_stitch_relation(relation=source('stitch_google_ads', 'ad_groups'),unique_column='id') }}
),
renamed as (

  SELECT cast(id as {{ dbt_utils.type_string() }}) as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
          cast (campaignid as {{ dbt_utils.type_string() }}) ad_campaign_id,
          cast (null as {{ dbt_utils.type_timestamp() }}) as adset_created_ts,
          cast (null as {{ dbt_utils.type_timestamp() }}) as adset_end_ts,
          cast (null as {{ dbt_utils.type_timestamp() }}) as adset_start_ts,
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
