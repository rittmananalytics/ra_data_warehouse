{% if not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='delivery_task_pk',
        alias='delivery_tasks_fact'
    )
}}
{% endif %}

WITH tasks AS
  (
  SELECT *
  FROM   {{ ref('int_delivery_tasks') }}
),
     projects AS
  (
    SELECT *
    FROM   {{ ref('wh_delivery_projects_dim') }}

  )
SELECT
   GENERATE_UUID() as delivery_task_pk,
   p.delivery_project_pk,
   t.* except (project_id),
FROM
   tasks t
LEFT OUTER JOIN projects p
   on t.project_id = p.project_id
