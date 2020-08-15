{% if not var("enable_google_ads_source") or not var("ad_campaigns_only") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% if var("stg_google_ads_etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_table(var('stg_google_ads_stitch_schema'),var('stg_google_ads_stitch_ad_groups_table'),'id') }}
),
renamed as (

  SELECT concat('{{ var('stg_google_ads_id-prefix') }}',id) as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
         concat('{{ var('stg_google_ads_id-prefix') }}',campaign_id) ad_campaign_id,
         'Adwords' as ad_network
  FROM source

)
{% elif var("stg_google_ads_etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('stg_google_ads_segment_schema'),var('stg_google_ads_segment_ad_groups_table')) }}
),
renamed as (
  SELECT concat('{{ var('stg_google_ads_id-prefix') }}',id) as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
         concat('{{ var('stg_google_ads_id-prefix') }}',campaign_id) ad_campaign_id,
         'Adwords' as ad_network
  FROM source )
{% endif %}
select
 *
from
 renamed
