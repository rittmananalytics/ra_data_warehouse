{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_source('stitch_facebook_ads','s_ads','id') }}
),

renamed as (

    select
    concat('facebook-ads-',id) as ad_id,
    name as ad_name,
    concat('facebook-ads-',adset_id) as adset_id,
    source_ad_id,
    concat('facebook-ads-',campaign_id) as campaign_id,
    concat('facebook-ads-',account_id) as account_id,
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
