with harvest_users as (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
    FROM
      {{ source('harvest_projects', 'users') }})
  WHERE
    latest_sdc_batched_at = _sdc_batched_at
)
select
       'harvest_projects'                 as source,
       concat('harvest-',id)              as user_id,
       concat(first_name,' ',last_name)   as user_name,
       email                              as user_email,
       is_contractor                      as user_is_contractor,
       true                               as user_is_staff,
       weekly_capacity                    as user_weekly_capacity,
       telephone                          as user_phone,
       default_hourly_rate                as user_default_hourly_rate,
       cost_rate                          as user_cost_rate,
       is_active                          as user_is_active,
       created_at                         as user_created_ts,
       updated_at                         as user_last_modified_ts
from harvest_users u
