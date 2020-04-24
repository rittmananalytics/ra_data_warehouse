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
        {{ source('stitch_facebook_ads', 's_adsets') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
        id as adset_id,
        name as adset_name,
        account_id as account_id,
        campaign_id as campaign_id,
        budget_remaining as adset_budget_remaining,
        effective_status as adset_effective_status,
        targeting as adset_targeting,
        created_time as adset_created_time,
        end_time as adset_end_ts,
        start_time as adset_start_ts,
        updated_time as adset_last_modified_ts
    from source
)

select * from renamed
