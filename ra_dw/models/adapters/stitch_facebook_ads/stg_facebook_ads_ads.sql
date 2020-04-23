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
        tracking_specs,
        effective_status,
        targeting,
        recommendations,
        campaign_id,
        conversion_specs,
        source_ad_id,
        updated_time,
        id,
        adset_id,
        bid_type,
        name,
        _sdc_table_version,
        created_time,
        status,
        _sdc_received_at,
        _sdc_sequence,
        last_updated_by_app_id,
        account_id,
        _sdc_batched_at,
        _sdc_extracted_at,
        creative

    from source

)

select * from renamed
