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
        concat('{{ var('id-prefix') }}',id)      as ad_campaign_id,
        name      as ad_campaign_name,
        effective_status as ad_campaign_status
        effective_status as ad_campaign_serving_status,
        start_time as ad_campaign_start_date,
        cast(null as timestamp) as ad_campaign_end_date
    from source
)
{% elif var("etl") == 'segment' %}
with source as (
  {{ filter_segment_table(var('segment_schema'),var('segment_campaigns_table')) }}
),
renamed as (
  select
  concat('{{ var('id-prefix') }}',id)      as ad_campaign_id,
  name      as ad_campaign_name,
  effective_status as ad_campaign_status
  effective_status as ad_campaign_serving_status,
  start_time as ad_campaign_start_date,
  stop_time as ad_campaign_end_date
)
{% endif %}
select * from renamed
