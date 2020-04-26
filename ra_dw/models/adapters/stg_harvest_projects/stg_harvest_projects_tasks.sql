{% if not var("enable_harvest_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  {{ filter_source('harvest_projects','s_tasks','id') }}

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
