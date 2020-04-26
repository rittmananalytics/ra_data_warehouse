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
        {{ source('stitch_facebook_ads', 's_campaigns') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
        id      as campaign_id,
        name      as campaign_name,
        account_id as account_id,
        objective as campaign_objective,
        effective_status as campaign_effective_status,
        buying_type as campaign_buying_type,
        start_time as campaign_start_ts,
        updated_time as campaign_last_modified_ts
    from source

)

select * from renamed
