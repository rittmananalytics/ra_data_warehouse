{% if not var("enable_facebook_ads_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
    (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('stitch_facebook_ads', 's_ads') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
    id as ad_id,
    name as ad_name,
    adset_id as adset_id,
    source_ad_id,
    campaign_id as campaign_id,
    account_id as account_id,
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
