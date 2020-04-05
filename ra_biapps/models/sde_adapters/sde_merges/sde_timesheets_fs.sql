with sde_timesheets_fs_merge_list as
  (
    SELECT * except (timesheet_id),
           timesheet_id as harvest_timesheet_id
    FROM   {{ ref('sde_harvest_projects_timesheets') }}
  )
select * from sde_timesheets_fs_merge_list
