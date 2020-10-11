{% if not var("enable_google_ads_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% if var("stg_google_ads_etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_table(var('stg_google_ads_stitch_schema'),var('stg_google_ads_stitch_campaigns_table'),'id') }}
),
renamed as (

    select
    cast(id as string)              as ad_campaign_id,
    name            as ad_campaign_name,
    status          as ad_campaign_status,
    cast(null as string) as campaign_buying_type,
    cast(null as timestamp)      as ad_campaign_start_date,
    cast(null as timestamp)        as ad_campaign_end_date,
    'Google Ads' as ad_network

    from source

)
{% elif var("stg_google_ads_etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('stg_google_ads_segment_schema'),var('stg_google_ads_segment_campaigns_table')) }}
),
renamed as (
SELECT
  cast(id as string)             as ad_campaign_id,
  name            as ad_campaign_name,
  status          as ad_campaign_status,
  cast(null as string) as campaign_buying_type,
  cast(null as timestamp)      as ad_campaign_start_date,
  cast(null as timestamp)        as ad_campaign_end_date,
  'Google Ads' as ad_network
FROM
  source)
{% endif %}
select
 *
from
 renamed
