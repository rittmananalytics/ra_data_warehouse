with sde_tasks_ds_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_jira_projects_tasks') }}
    UNION ALL
    SELECT *
    FROM   {{ ref('sde_asana_projects_tasks') }}
  )
select * from sde_tasks_ds_merge_list
