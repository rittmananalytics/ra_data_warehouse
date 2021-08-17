{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}

{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}


with source as (
  {{ filter_segment_relation(source('segment_google_ads', 'ads')) }}
),
renamed as (
SELECT
      cast(id as {{ dbt_utils.type_string() }})          as ad_id,
      status      as ad_status,
      type        as ad_type,
      final_urls  as ad_final_urls,
      cast(ad_group_id) as {{ dbt_utils.type_string() }}) as ad_group_id,
      cast(null as {{ dbt_utils.type_string() }}) as ad_bid_type,
      cast(null as {{ dbt_utils.type_string() }})  as ad_utm_parameters,
      cast(null as {{ dbt_utils.type_string() }})  as ad_utm_campaign,
      cast(null as {{ dbt_utils.type_string() }})  as ad_utm_content,
      cast(null as {{ dbt_utils.type_string() }})  as ad_utm_medium,
      cast(null as {{ dbt_utils.type_string() }})  as ad_utm_source,
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
