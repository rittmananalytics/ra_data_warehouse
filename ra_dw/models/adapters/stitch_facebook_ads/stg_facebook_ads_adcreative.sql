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
        {{ source('stitch_facebook_ads', 's_adcreative') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
        effective_object_story_id,
        id,
        actor_id,
        name,
        _sdc_table_version,
        status,
        _sdc_received_at,
        _sdc_sequence,
        object_story_spec,
        account_id,
        _sdc_batched_at,
        _sdc_extracted_at,
        object_type

    from source

)

select * from renamed
