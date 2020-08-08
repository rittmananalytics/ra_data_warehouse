{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% if var("etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_table(var('stitch_schema'),var('stitch_ad_groups_table'),'id') }}
),
renamed as (

  SELECT concat('{{ var('id-prefix') }}',id) as ad_group_id,
         name as ad_group_name,
         effective_status as ad_group_status,
         concat('{{ var('id-prefix') }}',campaignid) ad_campaign_id,
         targeting as adset_targeting,
         created_time as adset_created_ts,
         end_time as adset_end_ts,
         start_time as adset_start_ts,
         'Facebook Ads' as ad_network
  FROM source

)
{% elif var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_ad_groups_table')) }}
),
renamed as (
  SELECT concat('{{ var('id-prefix') }}',id) as ad_group_id,
         name as ad_group_name,
         effective_status as ad_group_status,
         concat('{{ var('id-prefix') }}',campaign_id) ad_campaign_id,
         'Facebook Ads' as ad_network
  FROM source )
{% endif %}
select
 *
from
 renamed
