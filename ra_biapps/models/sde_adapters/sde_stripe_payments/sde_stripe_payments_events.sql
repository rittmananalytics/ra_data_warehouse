{% if not enable_stripe_payments %}
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
        {{ source('stitch_stripe','events') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
        id as event_id,
        object as event_object,
        created as event_created_ts,
        api_version as event_api_version,
        request as event_request,
        pending_webhooks as event_pending_webhooks,
        livemode as event_livemode,
        type as event_type,
        updated as event_updated_ts,
        data as event_data

    from source

)

select * from renamed
