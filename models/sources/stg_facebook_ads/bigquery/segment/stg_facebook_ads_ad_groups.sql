{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_facebook_ads_etl") == 'segment')
   )
}}
{% if var("marketing_warehouse_ad_group_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_group_sources") %}


with source as (
  {{ filter_segment_relation(var('stg_facebook_ads_segment_ad_groups_table')) }}
),
renamed as (
  SELECT cast(id as string)  as ad_group_id,
         name as ad_group_name,
         effective_status as ad_group_status,
         cast(campaign_id as string) ad_campaign_id,
         'Facebook Ads' as ad_network
  FROM source )
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
