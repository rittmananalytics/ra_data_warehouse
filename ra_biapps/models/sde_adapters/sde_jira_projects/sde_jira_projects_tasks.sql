with tasks as (SELECT
  *
  FROM (
    select 'jira_projects'               as source,
    id as task_id,
    key as task_jira_task_code,
    fields.created  as task_created_ts,
    concat('jira-',fields.reporter.key) as task_creator_user_id,
    fields.project.id as project_id,
    fields.summary as task_name,
    fields.project.projecttypekey as task_project_code,
    fields.project.name as task_project_name,
    fields.project.key as task_project_key,
    fields.parent.key as task_parent_key,
    fields.progress.progress as task_progress,
    fields.progress.percent as task_progress_pct,
    fields.progress.total as task_progress_total,
    fields.aggregateprogress.progress as task_aggregate_progress,
    fields.aggregateprogress.percent as task_aggregate_progress_pct,
    fields.aggregateprogress.total as task_aggregate_progress_total,
    fields.timeestimate/3600 as task_estimated_hours,
    fields.timetracking.remainingestimateseconds/3600 as task_remaining_hours,
    fields.priority.name as task_priority,
    fields.issuetype.name as task_type,
    cast(null as string) as task_description,
    fields.status.statuscategory.colorname as task_colour_name,
    fields.status.statuscategory.name as task_status,
    fields.updated as task_last_modified_ts,
    timestamp(substr(fields.statuscategorychangedate,1,23)) task_status_change_ts,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
    {{ source('jira', 'issues') }}
  )
  WHERE
      _sdc_batched_at = max_sdc_batched_at
  )
select source,
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
