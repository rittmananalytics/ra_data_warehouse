with issues as (SELECT
  *
  FROM (
    select 'jira_projects'               as source,
    id as issue_id,
    key as issue_jira_issue_code,
    fields.created  as issue_created_ts,
    concat('jira-',fields.reporter.key) as issue_reporter_jira_users_key,
    fields.project.id as issue_project_id,
    fields.summary as issue_summary,
    fields.project.projecttypekey as issue_project_code,
    fields.project.name as issue_project_name,
    fields.project.key as issue_project_key,
    fields.parent.key as issue_parent_key,
    fields.progress.progress as issue_progress,
    fields.progress.percent as issue_progress_pct,
    fields.progress.total as issue_progress_total,
    fields.aggregateprogress.progress as issue_aggregate_progress,
    fields.aggregateprogress.percent as issue_aggregate_progress_pct,
    fields.aggregateprogress.total as issue_aggregate_progress_total,
    fields.timeestimate/3600 as issue_estimated_hours,
    fields.timetracking.remainingestimateseconds/3600 as issue_remaining_hours,
    fields.priority.name as issue_priority,
    fields.issuetype.name as issue_type,
    fields.status.description as issue_description,
    fields.status.statuscategory.colorname as issue_colour_name,
    fields.status.statuscategory.name as issue_status,
    fields.updated as issue_last_updated_ts,
    timestamp(substr(fields.statuscategorychangedate,1,23)) issue_status_change_ts,
    _sdc_batched_at,
    MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
  FROM
    {{ source('jira', 'issues') }}
  )
  WHERE
      _sdc_batched_at = max_sdc_batched_at
  )
select * from issues
