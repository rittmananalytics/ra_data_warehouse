with sde_delivery_projects_ds_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_jira_projects_projects') }}
    UNION ALL
    SELECT *
    FROM   {{ ref('sde_asana_projects_projects') }}
  )
select * from sde_delivery_projects_ds_merge_list
