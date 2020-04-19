{% if not var("enable_harvest_projects_source") %}
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
      MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source('harvest_projects', 's_tasks') }})
  WHERE
    max_sdc_batched_at = _sdc_batched_at
),
renamed as (
select
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
