{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}

{% if var("marketing_warehouse_ad_campaign_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_campaign_sources") %}

{% if var("stg_google_ads_etl") == 'segment' %}

with source as (
  {{ filter_segment_relation(source('segment_google_ads', 'campaigns')) }}
),
renamed as (
SELECT
  cast(id as {{ dbt_utils.type_string() }})             as ad_campaign_id,
  name            as ad_campaign_name,
  status          as ad_campaign_status,
  cast(null as {{ dbt_utils.type_string() }}) as campaign_buying_type,
   cast(null as {{ dbt_utils.type_timestamp() }})      as ad_campaign_start_date,
   cast(null as {{ dbt_utils.type_timestamp() }})        as ad_campaign_end_date,
  'Google Ads' as ad_network
FROM
  source)

{% elif var("stg_google_ads_etl") == 'stitch' %}

WITH source AS (
  {{ filter_stitch_relation(relation=source('stitch_google_ads', 'campaigns'),unique_column='id') }}

),
renamed as (

    select
    cast(id as {{ dbt_utils.type_string() }})              as ad_campaign_id,
    name            as ad_campaign_name,
    status          as ad_campaign_status,
    cast(null as {{ dbt_utils.type_string() }}) as campaign_buying_type,
     cast(null as {{ dbt_utils.type_timestamp() }})      as ad_campaign_start_date,
     cast(null as {{ dbt_utils.type_timestamp() }})        as ad_campaign_end_date,
    'Google Ads' as ad_network

    from source

{% endif %}

select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
