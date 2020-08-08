{% if not var("enable_google_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% if var("etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_table(var('stitch_schema'),var('stitch_campaigns_table'),'id') }}
),
renamed as (

    select
    concat('{{ var('id-prefix') }}',id)              as ad_campaign_id,
    name            as ad_campaign_name,
    status          as ad_campaign_status,
    cast(null as string) as campaign_buying_type,
    cast(null as timestamp)      as ad_campaign_start_date,
    cast(null as timestamp)        as ad_campaign_end_date,
    'Google Ads' as ad_network

    from source

)
{% elif var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_campaigns_table')) }}
),
renamed as (
SELECT
  concat('{{ var('id-prefix') }}',id)              as ad_campaign_id,
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
