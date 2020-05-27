{% if not var("enable_asana_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('tasks_table'),'gid') }}
),
renamed AS (
  SELECT
  concat('{{ var('id-prefix') }}',source.gid) as task_id,
  concat('{{ var('id-prefix') }}',projects.gid) AS project_id,
  concat('{{ var('id-prefix') }}',assignee.gid)  as task_creator_user_id,
  name  as task_name,
  cast(null as string) as task_priority,
  cast(null as string) task_type,
  notes as task_description,
  cast(null as string) task_status,
  completed   as task_is_completed,
  completed_at  as task_completed_ts,
  created_at    as task_created_ts,
  modified_at as task_last_modified_ts
  FROM
    source,
    unnest(projects) projects
  {{ dbt_utils.group_by(12) }}
)
SELECT
  *
FROM
  renamed
