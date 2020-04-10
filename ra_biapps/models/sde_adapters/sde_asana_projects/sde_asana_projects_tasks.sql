with tasks as (SELECT
  *
  FROM (
    SELECT
    'asana_projects' as source,
  t.gid as task_id,
  projects.gid AS project_id,
  concat('asana-',assignee.gid)  as task_creator_user_id,
  t.name  as task_name,
  cast(null as string) as task_priority,
  cast(null as string) task_type,
  notes as task_description,
  cast(null as string) task_status,
  completed   as task_is_completed,
  completed_at  as task_completed_ts,
  modified_at as task_last_modified_ts,
  due_on        as task_due_ts,
  created_at    as task_created_ts,
  _sdc_batched_at,
  MAX(_sdc_batched_at) OVER (PARTITION BY t.gid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
FROM
  {{ source('stitch_asana', 'tasks') }} t,
  unnest(projects) projects
)

  WHERE
      _sdc_batched_at = max_sdc_batched_at
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  )
select
      source,
       task_id,
       project_id,
       task_creator_user_id,
       task_name,
       task_priority,
       task_type,
       task_description,
       task_status,
       task_created_ts,
       task_last_modified_ts
 from tasks
