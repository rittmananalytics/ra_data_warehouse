{% if not enable_harvest_projects %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
    FROM
      {{ source('harvest_projects', 'tasks') }})
  WHERE
    latest_sdc_batched_at = _sdc_batched_at
),
renamed as (
select
       'harvest_projects'  as source,
       id                  as task_id,
       name                as task_name,
       billable_by_default as task_billable_by_default,
       default_hourly_rate as task_default_hourly_rate,
       created_at          as task_created_at,
       updated_at          as task_updated_at,
       is_active           as task_is_active
from source)
SELECT
  *
FROM
  renamed
