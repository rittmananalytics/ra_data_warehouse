{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('ads_table'),'id') }}

),

renamed as (

    select
    concat('{{ var('id-prefix') }}',id) as ad_id,
    name as ad_name,
    concat('{{ var('id-prefix') }}',adset_id) as adset_id,
    source_ad_id,
    concat('{{ var('id-prefix') }}',campaign_id) as campaign_id,
    concat('{{ var('id-prefix') }}',account_id) as account_id,
    bid_type as ad_bid_type,
    tracking_specs as ad_tracking_specs,
    effective_status as ad_effective_status,
    targeting as ad_targeting,
    recommendations as ad_recommendations,
    conversion_specs as ad_conversion_specs,
    status as ad_status,
    last_updated_by_app_id as ad_last_updated_by_app_id,
    creative as ad_creative,
    created_time as ad_created_ts,
    updated_time as ad_last_modified_ts,
    from source
)

select * from renamed
