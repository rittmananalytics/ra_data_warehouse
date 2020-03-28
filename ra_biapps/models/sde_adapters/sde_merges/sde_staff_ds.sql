with sde_staff_ds_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_harvest_projects_staff_ds') }}
  )
select * from sde_staff_ds_merge_list
