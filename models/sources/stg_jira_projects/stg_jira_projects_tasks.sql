{% if not var("enable_jira_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  {{ filter_stitch_table(var('stg_jira_projects_stitch_schema'),var('stg_jira_projects_stitch_issues_table'),'key') }}
),
renamed as (
select concat('{{ var('stg_jira_projects_id-prefix') }}',id) as task_id,
       fields.parent.id as parent_task_id,
       concat('{{ var('stg_jira_projects_id-prefix') }}',fields.project.id) as project_id,
       concat('{{ var('stg_jira_projects_id-prefix') }}',fields.reporter.accountid) as task_creator_user_id,
       concat('{{ var('stg_jira_projects_id-prefix') }}',fields.assignee.accountid) as task_assignee_user_id,
       fields.summary as task_name,
       fields.priority.name as task_priority,
       fields.issuetype.name as task_type,
       cast(null as string) as task_description,
       fields.status.statuscategory.name as task_status,
       timestamp(fields.resolutiondate) as task_resolution_ts,
       fields.resolution.name as task_resolution_type,
       fields.status.statuscategory.colorname	as task_status_colour,
       case when fields.status.statuscategory.name = 'Done' then true else false end as task_is_completed,
       case when fields.status.statuscategory.name = 'Done'
       or timestamp(fields.resolutiondate) is not null then coalesce(timestamp(fields.resolutiondate),timestamp(fields.statuscategorychangedate)) end  as task_completed_ts,
       timestamp(fields.statuscategorychangedate) as task_status_change_ts,
       case when fields.status.statuscategory.name = 'Done' or timestamp(fields.resolutiondate) is not null
        then timestamp_diff(coalesce(timestamp(fields.resolutiondate),timestamp(fields.statuscategorychangedate)),fields.created,,HOUR)
        end as total_task_hours_to_complete,
      case when fields.status.statuscategory.name <> 'Done' and timestamp(fields.resolutiondate) is null
         then timestamp_diff(current_timestamp,fields.created,,HOUR)
         end as total_task_hours_incomplete,
       case when fields.status.statuscategory.name = 'Done' then 1 end as total_delivery_tasks_completed,
       case when fields.status.statuscategory.name = 'In Progress' then 1 end as total_delivery_tasks_in_progress,
       case when fields.status.statuscategory.name = 'To Do' then 1 end as total_delivery_tasks_to_do,
       case when fields.priority.name = 'Low' then 1 end as total_delivery_priority_low,
       case when fields.priority.name = 'Medium' then 1 end as total_delivery_priority_medium,
       case when fields.priority.name = 'High' then 1 end as total_delivery_tasks_high,
       case when fields.issuetype.name = 'Task' then 1 end as total_delivery_tasks,
       case when fields.issuetype.name = 'Subtask' then 1 end as total_delivery_subtasks,
       1 as total_issues,
       fields.created  as task_created_ts,
       fields.updated as task_last_modified_ts,
 from source)
SELECT
 *
FROM
 renamed
