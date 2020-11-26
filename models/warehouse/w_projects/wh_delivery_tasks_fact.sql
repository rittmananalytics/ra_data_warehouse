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

  ),
  users as
  (
    SELECT *
    FROM  {{ ref('wh_users_dim') }}
  )
SELECT
   GENERATE_UUID() as delivery_task_pk,
   p.delivery_project_pk,
   u.user_pk,
   t.* except (project_id),
FROM
   tasks t
JOIN users u
      ON t.task_assignee_user_id IN UNNEST(u.all_user_ids)
LEFT OUTER JOIN projects p
   on t.project_id = p.project_id
