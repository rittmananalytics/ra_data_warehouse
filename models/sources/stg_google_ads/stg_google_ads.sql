{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}

{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}


with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_ads_table')) }}
),
renamed as (
SELECT
      {{ cast('id','string') }}          as ad_id,
      status      as ad_status,
      type        as ad_type,
      final_urls  as ad_final_urls,
      cast(ad_group_id as string) as ad_group_id,
      {{ cast() }} as ad_bid_type,
      {{ cast() }}  as ad_utm_parameters,
      {{ cast() }}  as ad_utm_campaign,
      {{ cast() }}  as ad_utm_content,
      {{ cast() }}  as ad_utm_medium,
      {{ cast() }}  as ad_utm_source,
      'Google Ads' as ad_network
FROM
  source)
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
