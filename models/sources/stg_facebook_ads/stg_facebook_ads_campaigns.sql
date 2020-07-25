{% if not var("enable_facebook_ads_source") %}
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
    concat('{{ var('id-prefix') }}',id)      as campaign_id,
    name      as campaign_name,
    status          as ad_campaign_status,
    effective_status as campaign_effective_status,
    start_time      as ad_campaign_start_date,
    stop_time        as ad_campaign_end_date,
    'Facebook Ads' as ad_network

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
  effective_status          as ad_campaign_status,
  buying_type as campaign_buying_type,
  start_time      as ad_campaign_start_date,
  stop_time        as ad_campaign_end_date,
  'Facebook Ads' as ad_network
FROM
  source)
{% endif %}
select
 *
from
 renamed
