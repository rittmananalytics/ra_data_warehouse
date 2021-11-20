{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_delivery_sources") %}
{% if 'asana_projects' in var("projects_warehouse_delivery_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_asana_projects_stitch_tasks_table'),unique_column='gid') }}

),
renamed AS (
  SELECT
  CONCAT('{{ var('stg_asana_projects_id-prefix') }}',source.gid)                      AS task_id,
  parent.gid                                                                          AS parent_task_id,
  CONCAT('{{ var('stg_asana_projects_id-prefix') }}',projects.value.gid)              AS project_id,
  coalesce(CONCAT('{{ var('stg_asana_projects_id-prefix') }}',assignee.gid),'-999')   AS task_creator_user_id,
  coalesce(CONCAT('{{ var('stg_asana_projects_id-prefix') }}',assignee.gid),'-999')   AS task_assignee_user_id,
  name                                                                                AS task_name,
  case when parent.gid is null then 'Task' else 'Subtask' end                         AS task_type,
  notes                                                                               AS task_description,
  CAST(null AS {{ dbt_utils.type_string() }})                                         AS task_url,
  CAST(null AS {{ dbt_utils.type_string() }})                                         AS task_status,
  null                                                                                AS task_status_workflow_stage_number,
  CAST(null AS {{ dbt_utils.type_string() }}) AS task_status_colour,
  completed   AS task_is_completed,
  completed_at  AS task_completed_ts,
  modified_at AS task_status_change_ts,
  {{ dbt_utils.datediff('created_at', 'completed_at', 'HOUR') }} total_task_hours_to_complete,
  null AS total_task_hours_incomplete,
  CAST(null AS {{ dbt_utils.type_string() }}) 	as deliverable_id,
  CAST(null AS {{ dbt_utils.type_string() }})  AS deliverable_type,
  CAST(null AS {{ dbt_utils.type_string() }})  AS deliverable_category,
  CAST(null AS {{ dbt_utils.type_string() }}) 	as sprint_name,
  CAST(null AS {{ dbt_utils.type_string() }})  AS sprint_board_url,
  CAST(null AS {{ dbt_utils.type_timestamp() }})  AS task_end_ts,
  CAST(null AS {{ dbt_utils.type_timestamp() }}) AS task_start_ts,
  CAST(null AS {{ dbt_utils.type_string() }})  AS sprint_goal,
  null AS total_completed,
  null AS total_in_progress,
  null AS total_failed_client_qa,
  null AS total_to_do,
  null AS total_blocked,
  null AS total_in_client_qa,
  null AS total_in_qa,
  null AS total_in_design,
  null AS total_in_add_to_looker,
  null AS total_in_looker_qa,
  case when case when parent.gid is null then 'Task' else 'Subtask' end = 'Task' then 1 end AS total_delivery_tasks,
  case when case when parent.gid is null then 'Task' else 'Subtask' end = 'Subtask' then 1 end AS total_delivery_subtasks,
  1 AS total_issues,
  created_at    AS task_created_ts,
  modified_at AS task_last_modified_ts
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
