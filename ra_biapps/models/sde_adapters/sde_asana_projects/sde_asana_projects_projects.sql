with projects as (SELECT
  *
  FROM (
  SELECT
    'asana_projects'               as source,
    concat('asana-',gid) as project_id,
    concat('asana-',owner.gid) as lead_user_id,
    name as project_name,
    team.gid as project_team_id,
    current_status as project_status,
    notes as project_notes,
    created_at as project_created_at_ts,
    modified_at as project_modified_at_ts,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY gid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
    {{ source('stitch_asana', 'projects') }})
WHERE
  _sdc_batched_at = max_sdc_batched_at)
select p.source,
       p.project_id,
       p.lead_user_id,
       p.project_name,
       p.project_status,
       p.project_notes,
       cast (null as string) as project_type,
       cast (null as string) as project_category_description,
       cast (null as string) as project_category_name,
       p.project_created_at_ts,
       p.project_modified_at_ts
from projects p
