{% if not var("enable_asana_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('stitch_schema'),var('stitch_tasks_table'),'gid') }}
),
renamed AS (
  SELECT
  concat('{{ var('id-prefix') }}',source.gid) as task_id,
  parent as parent_task_id,
  concat('{{ var('id-prefix') }}',projects.gid) AS project_id,
  concat('{{ var('id-prefix') }}',assignee.gid)  as task_creator_user_id,
  cast (null as string) as task_assignee_user_id,
  name  as task_name,
  cast(null as string) as task_priority,
  case when parent is null then 'Task' else 'Subtask' end as task_type,
  notes as task_description,
  cast(null as string) task_status,
  completed   as task_is_completed,
  completed_at  as task_completed_ts,
  timestamp_diff(task_completed_ts,task_created_ts,HOUR) total_task_hours_to_complete,
  case when task_status = 'Done' then 1 end as total_delivery_tasks_completed,
  case when task_status = 'In Progress' then 1 end as total_delivery_tasks_in_progress,
  case when task_status = 'To Do' then 1 end as total_delivery_tasks_to_do,
  case when task_priority = 'Low' then 1 end as total_delivery_priority_low,
  case when task_priority = 'Medium' then 1 end as total_delivery_priority_medium,
  case when task_priority = 'High' then 1 end as total_delivery_tasks_high,
  case when task_type = 'Task' then 1 end as total_delivery_tasks,
  case when task_type = 'Subtask' then 1 end as total_delivery_subtasks,
  created_at    as task_created_ts,
  modified_at as task_last_modified_ts
  FROM
    source,
    unnest(projects) projects
  {{ dbt_utils.group_by(13) }}
)
SELECT
  *
FROM
  renamed
where task_type != 'Subtask'
