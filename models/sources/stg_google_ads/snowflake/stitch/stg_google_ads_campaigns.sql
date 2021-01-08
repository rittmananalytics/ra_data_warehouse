{{config(enabled = target.type == 'snowflake')}}
{% if var("marketing_warehouse_ad_campaign_sources") %}
{% if 'google_ads' in var("marketing_warehouse_ad_campaign_sources") %}

{% if var("stg_google_ads_etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_google_ads_stitch_campaigns_table'),unique_column='id') }}

),
renamed as (

    select
    cast(id as string)              as ad_campaign_id,
    name            as ad_campaign_name,
    status          as ad_campaign_status,
    cast(null as string) as campaign_buying_type,
    start_date      as ad_campaign_start_date,
    end_date        as ad_campaign_end_date,
    'Google Ads' as ad_network

    from source

)
{% elif var("stg_google_ads_etl") == 'segment' %}
with source as (
  {{ filter_segment_relation(var('stg_google_ads_segment_campaigns_table')) }}
),
renamed as (
SELECT
  cast(id as string)             as ad_campaign_id,
  name            as ad_campaign_name,
  status          as ad_campaign_status,
  cast(null as string) as campaign_buying_type,
  start_date      as ad_campaign_start_date,
  end_date       as ad_campaign_end_date,
  'Google Ads' as ad_network
FROM
  source)
{% endif %}
select
 *
from
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
