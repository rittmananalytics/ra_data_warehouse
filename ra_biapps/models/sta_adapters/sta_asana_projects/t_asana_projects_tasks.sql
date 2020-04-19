{% if not var("enable_asana_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
    (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY gid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('stitch_asana','s_tasks') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
renamed AS (
  SELECT
  source.gid as task_id,
  projects.gid AS project_id,
  concat('asana-',assignee.gid)  as task_creator_user_id,
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
  GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12
)
SELECT
  *
FROM
  renamed
