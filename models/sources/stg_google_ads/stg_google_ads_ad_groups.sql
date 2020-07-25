{% if not var("enable_google_ads_source") %}
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
         status as ad_group_status,
         concat('{{ var('id-prefix') }}',campaign_id) ad_campaign_id,
         'Adwords' as ad_network
  FROM source

)
{% elif var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_ad_groups_table')) }}
),
renamed as (
  SELECT concat('{{ var('id-prefix') }}',id) as ad_group_id,
         name as ad_group_name,
         status as ad_group_status,
         concat('{{ var('id-prefix') }}',campaign_id) ad_campaign_id,
         'Adwords' as ad_network
  FROM source )
{% endif %}
select
 *
from
 renamed
