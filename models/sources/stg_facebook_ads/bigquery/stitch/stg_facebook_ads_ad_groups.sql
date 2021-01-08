{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_facebook_ads_etl") == 'stitch')
   )
}}
{% if var("marketing_warehouse_ad_group_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_group_sources") %}

WITH source AS (
{{ filter_stitch_relation(relation=var('stg_facebook_ads_stitch_ad_groups_table'),unique_column='id') }}

),
renamed as (

  SELECT cast(id as string) as ad_group_id,
         name as ad_group_name,
         effective_status as ad_group_status,
         cast(campaignid as string) ad_campaign_id,
         targeting as adset_targeting,
         created_time as adset_created_ts,
         end_time as adset_end_ts,
         start_time as adset_start_ts,
         'Facebook Ads' as ad_network
  FROM source

)
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
