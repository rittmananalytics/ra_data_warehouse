{% if not var("enable_harvest_projects_source") or (not var("enable_projects_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='timesheet_projects_pk',
        alias='timesheets_fact'
    )
}}
{% endif %}

with companies_dim as (
    select *
    from {{ ref('wh_companies_dim') }}
),

user_dim as (
    select *
    from {{ ref('wh_users_dim') }}
),
  tasks_dim as (
      select *
      from {{ ref('wh_timesheet_tasks_dim') }}
)
,
  projects_dim as (
      select *
      from {{ ref('wh_timesheet_projects_dim') }}
)
,
  timesheets as (
      select *
      from {{ ref('int_timesheets') }}
)
SELECT

    GENERATE_UUID() as timesheet_pk,
    c.company_pk,
    s.user_pk,
    p.timesheet_project_pk,
    ta.timesheet_task_pk,
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
   timesheets t
JOIN companies_dim c
   ON cast(t.company_id as string) IN UNNEST(c.all_company_ids)
LEFT OUTER JOIN projects_dim p
   ON concat('harvest-',cast(t.timesheet_project_id as string)) = p.timesheet_project_id
LEFT OUTER JOIN tasks_dim ta
   ON t.timesheet_task_id = ta.task_id
JOIN user_dim s
   ON cast(t.timesheet_users_id as string) IN UNNEST(s.all_user_ids)
