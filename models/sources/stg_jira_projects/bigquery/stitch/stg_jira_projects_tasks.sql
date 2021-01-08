{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_delivery_sources") %}
{% if 'jira_projects' in var("projects_warehouse_delivery_sources") %}


with source as (
  {{ filter_stitch_relation(relation=var('stg_jira_projects_stitch_issues_table'),unique_column='key') }}
),
renamed as (
select concat('{{ var('stg_jira_projects_id-prefix') }}',id) as task_id,
       fields.parent.id as parent_task_id,
       concat('{{ var('stg_jira_projects_id-prefix') }}',fields.project.id) as project_id,
       coalesce(concat('{{ var('stg_jira_projects_id-prefix') }}',fields.reporter.accountid),concat('{{ var('stg_jira_projects_id-prefix') }}','-999')) as task_creator_user_id,
       coalesce(concat('{{ var('stg_jira_projects_id-prefix') }}',fields.assignee.accountid),concat('{{ var('stg_jira_projects_id-prefix') }}','-999')) as task_assignee_user_id,
       fields.summary as task_name,
       fields.issuetype.name as task_type,
       cast(null as string) as task_description,
       concat('{{ var('stg_jira_projects_jira_url') }}','/software/projects/',fields.project.key,'/issues/', key) as task_url,
       fields.status.name	 as task_status,
       case when fields.status.name	 = 'To Do' then 1
            when fields.status.name	 = 'Design & Validation' then 2
            when fields.status.name	 = 'Blocked' then 3
            when fields.status.name	 = 'In Progress' then 4
            when fields.status.name	 = 'In QA' then 5
            when fields.status.name	 = 'Add to Looker' then 6
            when fields.status.name  = 'Looker Internal QA ' then 7
            when fields.status.name	 = 'In Client QA' then 8
            when fields.status.name  = 'Failed Client QA/QA Comment' then 9
            when fields.status.name	 in ('Done','Done/Passed Client QA') then 10
            else null end as task_status_workflow_stage_number,
       fields.status.statuscategory.colorname	as task_status_colour,
       case when fields.status.name	 in ('Done','Done/Passed Client QA') then true else false end as task_is_completed,
       case when fields.status.name	 in ('Done','Done/Passed Client QA') then timestamp(fields.statuscategorychangedate) end  as task_completed_ts,
       timestamp(fields.statuscategorychangedate) as task_status_change_ts,
       case when fields.status.name	 in ('Done','Done/Passed Client QA') then timestamp_diff(timestamp(fields.statuscategorychangedate),fields.created,HOUR)
        end as total_task_hours_to_complete,
      case when fields.status.name	 not in ('Done','Done/Passed Client QA') then timestamp_diff({{ dbt_utils.current_timestamp() }},fields.created,HOUR)
         end as total_task_hours_incomplete,
       fields.customfield_10135	as deliverable_id,
       fields.customfield_10136.value as deliverable_type,
       fields.customfield_10137.value as deliverable_category,
       customfield_10018.value.name	as sprint_name,
       concat('{{ var('stg_jira_projects_jira_url') }}','/software/projects/',fields.project.key,'/boards/', customfield_10018.value.boardid) as sprint_board_url,
       timestamp(customfield_10018.value.enddate) as task_end_ts,
       timestamp(customfield_10018.value.startdate) as task_start_ts,
       customfield_10018.value.goal as sprint_goal,
       case when fields.status.name	 in ('Done','Done/Passed Client QA') then 1 end as total_completed,
       case when fields.status.name	 = 'In Progress' then 1 end as total_in_progress,
       case when fields.status.name  = 'Failed Client QA/QA Comment' then 1 end as total_failed_client_qa,
       case when fields.status.name	 = 'To Do' then 1 end as total_to_do,
       case when fields.status.name	 = 'Blocked' then 1 end as total_blocked,
       case when fields.status.name	 = 'In Client QA' then 1 end as total_in_client_qa,
       case when fields.status.name	 = 'In QA' then 1 end as total_in_qa,
       case when fields.status.name	 = 'Design & Validation' then 1 end as total_in_design,
       case when fields.status.name	 = 'Add to Looker' then 1 end as total_in_add_to_looker,
       case when fields.status.name  = 'Looker Internal QA' then 1 end as total_in_looker_qa,
       case when fields.issuetype.name = 'Task' then 1 end as total_delivery_tasks,
       case when fields.issuetype.name = 'Subtask' then 1 end as total_delivery_subtasks,
       1 as total_issues,
       fields.created  as task_created_ts,
       fields.updated as task_last_modified_ts,
 from source,
  unnest(fields.customfield_10018) as customfield_10018)
SELECT
 *
FROM
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
