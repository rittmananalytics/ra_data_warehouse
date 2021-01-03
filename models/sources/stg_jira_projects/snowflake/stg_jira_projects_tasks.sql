{{config(enabled = target.type == 'snowflake')}}
{% if var("projects_warehouse_delivery_sources") %}
{% if 'jira_projects' in var("projects_warehouse_delivery_sources") %}


with source as (
  {{ filter_stitch_relation(relation=var('stg_jira_projects_stitch_issues_table'),unique_column='key') }}
),
renamed as (
select concat('{{ var('stg_jira_projects_id-prefix') }}',id) as task_id,
       fields:parent:id::STRING as parent_task_id,
       concat('{{ var('stg_jira_projects_id-prefix') }}',fields:project:id::STRING) as project_id,
       coalesce(concat('{{ var('stg_jira_projects_id-prefix') }}',fields:reporter:accountid::STRING),concat('{{ var('stg_jira_projects_id-prefix') }}','-999')) as task_creator_user_id,
       coalesce(concat('{{ var('stg_jira_projects_id-prefix') }}',fields:assignee:accountid::STRING),concat('{{ var('stg_jira_projects_id-prefix') }}','-999')) as task_assignee_user_id,
       fields:summary::STRING as task_name,
       fields:issuetype:name::STRING as task_type,
       cast(null as string) as task_description,
       concat('{{ var('stg_jira_projects_jira_url') }}','/software/projects/',fields:project:key::STRING,'/issues/', key) as task_url,
       fields:status:name::string	 as task_status,
       case when fields:status:name::string	 = 'To Do' then 1
            when fields:status:name::string	 = 'Design & Validation' then 2
            when fields:status:name::string	 = 'Blocked' then 3
            when fields:status:name::string	 = 'In Progress' then 4
            when fields:status:name::string	 = 'In QA' then 5
            when fields:status:name::string	 = 'Add to Looker' then 6
            when fields:status:name::string  = 'Looker Internal QA ' then 7
            when fields:status:name::string	 = 'In Client QA' then 8
            when fields:status:name::string  = 'Failed Client QA/QA Comment' then 9
            when fields:status:name::string	 in ('Done','Done/Passed Client QA') then 10
            else null end as task_status_workflow_stage_number,
       fields:status:statuscategory:colorname::STRING	as task_status_colour,
       case when fields:status:name::string	 in ('Done','Done/Passed Client QA') then true else false end as task_is_completed,
       case when fields:status:name::string	 in ('Done','Done/Passed Client QA') then to_timestamp(split_part(replace(fields:statuscategorychangedate::STRING,'T',' '),'.',1),'yyyy-mm-dd HH:MI:SS') end  as task_completed_ts,
       to_timestamp(split_part(replace(fields:statuscategorychangedate::STRING,'T',' '),'.',1),'yyyy-mm-dd HH:MI:SS') as task_status_change_ts,
       cast (null as int) as total_task_hours_to_complete,
      cast (null as int) as total_task_hours_incomplete,
       fields:customfield_10135::STRING	as deliverable_id,
       fields:customfield_10136:value::STRING as deliverable_type,
       fields:customfield_10137:value::STRING as deliverable_category,
       fields:customfield_10018:value:name::STRING	as sprint_name,
       concat('{{ var('stg_jira_projects_jira_url') }}','/software/projects/',fields:project.key,'/boards/', fields:customfield_10018:value:boardid::STRING) as sprint_board_url,
       to_timestamp(split_part(replace(fields:customfield_10018:value:enddate::STRING,'T',' '),'.',1),'yyyy-mm-dd HH:MI:SS') as task_end_ts,
       to_timestamp(split_part(replace(fields:customfield_10018:value:startdate::STRING,'T',' '),'.',1),'yyyy-mm-dd HH:MI:SS') as task_start_ts,
       fields:customfield_10018:value:goal::STRING as sprint_goal,
       case when fields:status:name::string	 in ('Done','Done/Passed Client QA') then 1 end as total_completed,
       case when fields:status:name::string	 = 'In Progress' then 1 end as total_in_progress,
       case when fields:status:name::string  = 'Failed Client QA/QA Comment' then 1 end as total_failed_client_qa,
       case when fields:status:name::string	 = 'To Do' then 1 end as total_to_do,
       case when fields:status:name::string	 = 'Blocked' then 1 end as total_blocked,
       case when fields:status:name::string	 = 'In Client QA' then 1 end as total_in_client_qa,
       case when fields:status:name::string	 = 'In QA' then 1 end as total_in_qa,
       case when fields:status:name::string	 = 'Design & Validation' then 1 end as total_in_design,
       case when fields:status:name::string	 = 'Add to Looker' then 1 end as total_in_add_to_looker,
       case when fields:status:name::string  = 'Looker Internal QA' then 1 end as total_in_looker_qa,
       case when fields:issuetype.name = 'Task' then 1 end as total_delivery_tasks,
       case when fields:issuetype.name = 'Subtask' then 1 end as total_delivery_subtasks,
       1 as total_issues,

       to_timestamp(split_part(replace(fields:created:STRING,'T',' '),'.',1),'yyyy-mm-dd HH:MI:SS')  as task_created_ts,
       to_timestamp(split_part(replace(fields:updated:STRING,'T',' '),'.',1),'yyyy-mm-dd HH:MI:SS') as task_last_modified_ts
 from source)
SELECT
 *
FROM
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
