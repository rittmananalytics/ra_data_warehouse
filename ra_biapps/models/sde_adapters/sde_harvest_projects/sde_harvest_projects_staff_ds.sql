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
       'harvest_projects'               as source,
       id                               as staff_id,
       concat(first_name,' ',last_name) as staff_full_name,
       email                            as staff_email,
       is_contractor                    as staff_is_contractor,
       weekly_capacity                  as staff_weekly_capacity,
       telephone                        as staff_phone,
       default_hourly_rate              as staff_default_hourly_rate,
       cost_rate                        as staff_cost_rate,
       is_active                        as staff_is_active,
       created_at                       as staff_created_date,
       updated_at                       as staff_last_modified_date
from harvest_users u
