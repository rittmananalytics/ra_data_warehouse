with sde_timesheets_fs_merge_list as
  (
    SELECT * except (id),
           id as harvest_timesheet_id
    FROM   {{ ref('sde_harvest_projects_timesheets_fs') }}
  )
select * from sde_timesheets_fs_merge_list
