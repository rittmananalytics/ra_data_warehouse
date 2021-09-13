{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}

{% if var("marketing_warehouse_ad_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_sources") %}

{% if var("stg_google_ads_etl") == 'segment' %}


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

  {% elif var("stg_google_ads_etl") == 'stitch' %}

  WITH source AS (
    {{ filter_stitch_relation(relation=source('stitch_google_ads', 'AD_PERFORMANCE_REPORT'),unique_column='adid') }}
  ),
  renamed as (
      SELECT
        adid as ad_id,
        adstate as ad_status,
        adtype as ad_type,
        cast(null as {{ dbt_utils.type_string() }}) as ad_final_urls,
        adgroupid as ad_group_id,
        cast(null as {{ dbt_utils.type_string() }}) as ad_bid_type,
        cast(null as {{ dbt_utils.type_string() }})  as ad_utm_parameters,
        cast(null as {{ dbt_utils.type_string() }})  as ad_utm_campaign,
        cast(null as {{ dbt_utils.type_string() }}) as ad_utm_content,
        cast(null as {{ dbt_utils.type_string() }})  as ad_utm_medium,
        cast(null as {{ dbt_utils.type_string() }})  as ad_utm_source,
        'Google Ads' as ad_network       ,
      FROM
        source)
  {% endif %}
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
