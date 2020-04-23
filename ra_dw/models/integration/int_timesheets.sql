{% if not var("enable_harvest_projects_source") or not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_timesheets_merge_list as
  (
    SELECT * except (timesheet_id),
           timesheet_id as harvest_timesheet_id
    FROM   {{ ref('stg_harvest_projects_timesheets') }}
  )
select * from t_timesheets_merge_list
