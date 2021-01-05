{% if var("projects_warehouse_timesheet_sources") %}
{{
    config(
        unique_key='timesheet_task_pk',
        alias='timesheet_tasks_dim'
    )
}}


WITH tasks AS
  (
  SELECT {{ dbt_utils.star(from=ref('int_timesheet_tasks')) }}
  FROM   {{ ref('int_timesheet_tasks') }}
  )
SELECT
{{ dbt_utils.surrogate_key(['task_id']) }} as timesheet_task_pk,
   t.task_id,
   t.task_name,
   t.task_billable_by_default,
   t.task_default_hourly_rate,
   t.task_created_at,
   t.task_updated_at,
   t.task_is_active
FROM
   tasks t

{% else %}

{{config(enabled=false)}}

{% endif %}
