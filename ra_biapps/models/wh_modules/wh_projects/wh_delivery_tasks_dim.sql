{% if not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='timesheet_projects_pk',
        alias='delivery_tasks_dim'
    )
}}
{% endif %}

WITH tasks AS
  (
  SELECT *
  FROM   {{ ref('i_delivery_tasks_ds') }}
  )
SELECT
   GENERATE_UUID() as task_pk,
   t.*
FROM
   tasks t
