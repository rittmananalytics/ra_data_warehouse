{{
    config(
        alias='timesheets_fact'
    )
}}
with companies_dim as (
    select *
    from {{ ref('sil_companies_dim') }}
),
  staff_dim as (
    select *
    from {{ ref('sil_staff_dim') }}
),
  tasks_dim as (
      select *
      from {{ ref('sil_tasks_dim') }}
)
,
  projects_dim as (
      select *
      from {{ ref('sil_projects_dim') }}
)
,
  timesheets_fs as (
      select *
      from {{ ref('sde_timesheets_fs') }}
)
SELECT

    GENERATE_UUID() as timesheet_pk,
    c.company_pk,
    t.source,
    s.staff_pk,
    p.project_pk,
    ta.task_pk,
    timesheet_invoice_id,
    timesheet_billing_date,
    timesheet_hours_billed,
    timesheet_total_amount_billed,
    timesheet_is_billable,
    timesheet_has_been_billed,
    timesheet_has_been_locked,
    timesheet_billable_hourly_rate_amount,
    timesheet_billable_hourly_cost_amount,
    timesheet_notes
FROM
   timesheets_fs t
JOIN companies_dim c
   ON cast(t.company_id as string) IN UNNEST(c.all_company_ids)
LEFT OUTER JOIN projects_dim p
   ON t.timesheet_project_id = p.harvest_project_id
LEFT OUTER JOIN tasks_dim ta
   ON t.timesheet_task_id = ta.harvest_task_id
LEFT OUTER JOIN staff_dim s
   ON t.timesheet_staff_id = s.harvest_staff_id
