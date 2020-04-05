{{
    config(
        unique_key='project_pk',
        alias='timesheet_projects_dim'
    )
}}
WITH unique_projects AS
  (
  SELECT lower(project_name) as project_name
  FROM   {{ ref('sde_timesheet_projects_ds') }}
  GROUP BY 1
  )
,
  unique_projects_with_uuid AS
  (
  SELECT project_name,
         GENERATE_UUID() as project_uid
  FROM   unique_projects
  )

SELECT
   p.source,
   GENERATE_UUID() as project_pk,
   u.project_uid,
   p.project_id as harvest_project_id,
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
   {{ ref('sde_timesheet_projects_ds') }} p
JOIN unique_projects_with_uuid  u
ON lower(p.project_name) = u.project_name
