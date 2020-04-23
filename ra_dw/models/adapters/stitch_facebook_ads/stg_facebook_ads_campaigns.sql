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
        effective_status,
        updated_time,
        id,
        name,
        _sdc_table_version,
        objective,
        _sdc_received_at,
        _sdc_sequence,
        buying_type,
        start_time,
        account_id,
        _sdc_batched_at,
        ads,
        _sdc_extracted_at

    from source

)

select * from renamed
