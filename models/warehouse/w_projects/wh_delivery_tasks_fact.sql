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
  SELECT {{ dbt_utils.star(from=ref('int_delivery_tasks')) }}
  FROM   {{ ref('int_delivery_tasks') }}
),
     projects AS
  (
    SELECT {{ dbt_utils.star(from=ref('wh_delivery_projects_dim')) }}
    FROM   {{ ref('wh_delivery_projects_dim') }}

  ),
  contacts as
  (
    SELECT {{ dbt_utils.star(from=ref('wh_contacts_dim')) }}
    FROM  {{ ref('wh_contacts_dim') }}
  )
SELECT
   GENERATE_UUID() as delivery_task_pk,
   p.delivery_project_pk,
   c.contact_pk,
   t.* except (project_id),
FROM
   tasks t
JOIN contacts c
      ON t.task_assignee_user_id IN UNNEST(c.all_contact_ids)
LEFT OUTER JOIN projects p
   on t.project_id = p.project_id
