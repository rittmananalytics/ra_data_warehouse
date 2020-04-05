{{
    config(
        unique_key='user_pk',
        alias='tasks_dim'
    )
}}
WITH unique_tasks AS
  (
  SELECT lower(task_name) as task_name
  FROM   {{ ref('sde_tasks_ds') }}
  GROUP BY 1
  )
,
  unique_tasks_with_uuid AS
  (
  SELECT task_name,
         GENERATE_UUID() as task_uid
  FROM   unique_tasks
  )

SELECT
   t.source,
   GENERATE_UUID() as task_pk,
   t.task_id as harvest_task_id,
   t.task_name,
   t.task_billable_by_default,
   t.task_default_hourly_rate,
   t.task_created_at,
   t.task_updated_at,
   t.task_is_active
FROM
   {{ ref('sde_tasks_ds') }} t
JOIN unique_tasks_with_uuid  u
ON lower(t.task_name) = u.task_name
