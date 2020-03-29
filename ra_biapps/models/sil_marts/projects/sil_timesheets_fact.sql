{{
    config(
        alias='timesheets_fact'
    )
}}
SELECT
    t.source,
    GENERATE_UUID() as timesheet_pk,
    c.company_pk,
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
   {{ ref('sde_timesheets_fs') }} t
JOIN {{ ref('sil_companies_dim') }} c
   ON cast(t.timesheet_company_id as string) IN UNNEST(c.all_company_ids)
LEFT OUTER JOIN {{ ref('sil_projects_dim') }} p
   ON t.timesheet_project_id = p.harvest_project_id
LEFT OUTER JOIN {{ ref('sil_tasks_dim') }} ta
   ON t.timesheet_task_id = ta.harvest_task_id
LEFT OUTER JOIN {{ ref('sil_staff_dim') }} s
   ON t.timesheet_staff_id = s.harvest_staff_id
