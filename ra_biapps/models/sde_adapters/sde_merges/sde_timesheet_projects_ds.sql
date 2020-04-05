with sde_projects_ds_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_harvest_projects_projects') }}
  )
select * from sde_projects_ds_merge_list
