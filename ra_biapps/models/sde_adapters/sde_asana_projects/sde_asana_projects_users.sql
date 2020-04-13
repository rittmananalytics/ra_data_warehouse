{% if not var("enable_asana_projects") %}
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
        MAX(_sdc_batched_at) OVER (PARTITION BY gid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('stitch_asana','users') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
renamed AS (
  SELECT
  concat('asana-',gid)           as user_id,
  name                   as user_name  ,
  email                  as user_email ,
  cast(null as boolean)         as user_is_contractor,
  case when email like '%@rittmananalytics.com%' or email like '%mjr-analytics.com%' then true else false end as user_is_staff,
  cast(null as int64)           as user_weekly_capacity,
  cast(null as string)          as user_phone,
  cast(null as int64)           as user_default_hourly_rate,
  cast(null as int64)           as user_cost_rate,
  true                          as user_is_active,
  cast(null as timestamp)       as user_created_ts,
  cast(null as timestamp)       as user_last_modified_ts
  FROM
    source
  WHERE
    name NOT LIKE 'Private User'
)
SELECT
  *
FROM
  renamed
