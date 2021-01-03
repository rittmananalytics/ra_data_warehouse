{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_delivery_sources") %}
{% if 'asana_projects' in var("projects_warehouse_delivery_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_asana_projects_stitch_tasks_table'),unique_column='gid') }}

),
renamed AS (
  SELECT
  concat('{{ var('stg_asana_projects_id-prefix') }}',source.gid) as task_id,
  parent.gid as parent_task_id,
  concat('{{ var('stg_asana_projects_id-prefix') }}',projects.value.gid) AS project_id,
  coalesce(concat('{{ var('stg_asana_projects_id-prefix') }}',assignee.gid),'-999')  as task_creator_user_id,
  coalesce(concat('{{ var('stg_asana_projects_id-prefix') }}',assignee.gid),'-999') as task_assignee_user_id,
  name  as task_name,
  case when parent.gid is null then 'Task' else 'Subtask' end as task_type,
  notes as task_description,
  cast(null as string) as task_url,
  cast(null as string) task_status,
  null as task_status_workflow_stage_number,
  cast(null as string) as task_status_colour,
  completed   as task_is_completed,
  completed_at  as task_completed_ts,
  modified_at as task_status_change_ts,
  timestamp_diff(completed_at,created_at,HOUR) total_task_hours_to_complete,
  null as total_task_hours_incomplete,
  cast(null as string) 	as deliverable_id,
  cast(null as string)  as deliverable_type,
  cast(null as string)  as deliverable_category,
  cast(null as string) 	as sprint_name,
  cast(null as string)  as sprint_board_url,
  cast(null as timestamp)  as task_end_ts,
  cast(null as timestamp) as task_start_ts,
  cast(null as string)  as sprint_goal,
  null as total_completed,
  null as total_in_progress,
  null as total_failed_client_qa,
  null as total_to_do,
  null as total_blocked,
  null as total_in_client_qa,
  null as total_in_qa,
  null as total_in_design,
  null as total_in_add_to_looker,
  null as total_in_looker_qa,
  case when case when parent.gid is null then 'Task' else 'Subtask' end = 'Task' then 1 end as total_delivery_tasks,
  case when case when parent.gid is null then 'Task' else 'Subtask' end = 'Subtask' then 1 end as total_delivery_subtasks,
  1 as total_issues,
  created_at    as task_created_ts,
  modified_at as task_last_modified_ts
  FROM
    source,
    unnest(projects) projects
  {{ dbt_utils.group_by(40) }}
)
SELECT
  *
FROM
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
