{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("marketing_warehouse_ad_group_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_group_sources") %}

{% if var("stg_facebook_ads_etl") == 'segment' %}

with source as (
  {{ filter_segment_relation(var('stg_facebook_ads_segment_ad_groups_table')) }}
),
renamed as (
  SELECT {{ cast('id','string') }}  as ad_group_id,
         name as ad_group_name,
         effective_status as ad_group_status,
          {{ cast('campaign_id','string') }} as ad_campaign_id,
         'Facebook Ads' as ad_network
  FROM source )

{% elif var("stg_facebook_ads_etl") == 'stitch' %}

WITH source AS (
{{ filter_stitch_relation(relation=var('stg_facebook_ads_stitch_ad_groups_table'),unique_column='id') }}

),
renamed as (

  SELECT {{ cast('id','string') }} as ad_group_id,
         name as ad_group_name,
         effective_status as ad_group_status,
          {{ cast('campaign_id','string') }} ad_campaign_id,
         created_time as adset_created_ts,
         end_time as adset_end_ts,
         start_time as adset_start_ts,
         'Facebook Ads' as ad_network
  FROM source

)
{% else %}
    {{ exceptions.raise_compiler_error(var("stg_facebook_ads_etl") ~" not supported in this data source") }}
{% endif %}
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
