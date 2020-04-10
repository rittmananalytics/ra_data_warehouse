{{
    config(
        unique_key='timesheet_project_pk',
        alias='timesheet_projects_dim'
    )
}}
WITH timesheet_projects AS
  (
  SELECT *
  FROM   {{ ref('sde_timesheet_projects_ds') }}
  )
SELECT
   GENERATE_UUID() as timesheet_project_pk,
   p.source,
   p.timesheet_project_id,
   p.project_name,
   p.project_code,
   p.project_delivery_start_ts,
   p.project_delivery_end_ts,
   p.project_is_active,
   p.project_is_billable,
   p.project_hourly_rate,
   p.project_cost_budget,
   p.project_is_fixed_fee,
   p.project_is_expenses_included_in_cost_budget,
   p.project_fee_amount,
   p.project_budget_amount,
   p.project_over_budget_notification_pct,
   p.project_budget_by,
   p.project_client_id
FROM
   timesheet_projects p
