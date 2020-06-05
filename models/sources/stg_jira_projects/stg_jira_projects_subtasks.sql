{% if not var("enable_jira_projects_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  {{ filter_stitch_table(var('stitch_schema'),var('stitch_issues_table'),'key') }}
),
renamed as (
select concat('{{ var('id-prefix') }}',id) as task_id,
       fields.parent.id as parent_task_id,
       concat('{{ var('id-prefix') }}',fields.project.id) as project_id,
       concat('{{ var('id-prefix') }}',fields.reporter.accountid) as task_creator_user_id,
       concat('{{ var('id-prefix') }}',fields.assignee.accountid) as task_assignee_user_id,
       fields.summary as task_name,
       fields.priority.name as task_priority,
       fields.issuetype.name as task_type,
       cast(null as string) as task_description,
       fields.status.statuscategory.name as task_status,
       cast(null as boolean) as task_is_completed,
       cast(null as timestamp)  as task_completed_ts,
       timestamp_diff(task_completed_ts,task_created_ts,HOUR) total_task_hours_to_complete,
       case when task_status = 'Done' then 1 end as total_delivery_tasks_completed,
       case when task_status = 'In Progress' then 1 end as total_delivery_tasks_in_progress,
       case when task_status = 'To Do' then 1 end as total_delivery_tasks_to_do,
       case when task_priority = 'Low' then 1 end as total_delivery_priority_low,
       case when task_priority = 'Medium' then 1 end as total_delivery_priority_medium,
       case when task_priority = 'High' then 1 end as total_delivery_tasks_high,
       case when task_type = 'Task' then 1 end as total_delivery_tasks,
       case when task_type = 'Subtask' then 1 end as total_delivery_subtasks
       fields.created  as task_created_ts,
       fields.updated as task_last_modified_ts,
 from source)
SELECT
 *
FROM
 renamed
where task_type = 'Subtask'
