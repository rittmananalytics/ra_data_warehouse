{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_facebook_ads_etl") == 'segment')
   )
}}

{% if var("marketing_warehouse_ad_campaign_sources") %}
{% if 'facebook_ads' in var("marketing_warehouse_ad_campaign_sources") %}

with source as (
  {{ filter_segment_relation(relation=var('stg_facebook_ads_segment_campaigns_table')) }}
),
renamed as (
SELECT
  cast(id as string)              as ad_campaign_id,
  name                            as ad_campaign_name,
  effective_status                as ad_campaign_status,
  buying_type                     as campaign_buying_type,
  start_time                      as ad_campaign_start_date,
  stop_time                       as ad_campaign_end_date,
  'Facebook Ads'                  as ad_network
FROM
  source)
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
