{% if not var("enable_harvest_projects_source") or (not var("enable_projects_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='timesheets_rpt'
    )
}}
{% endif %}
with report as (select
company_pk,
company_name,
company_description,
timesheet_project_pk,
exchange_rate,
total_revenue as total_revenue,
project_budget_amount,
project_name,
project_duration_weeks,
project_code,
max(project_delivery_start_ts) as project_delivery_start_ts ,
max(project_is_active) as project_is_active,
max(project_budget_by) as project_budget_by,
max(round((project_fee_amount*exchange_rate))) as project_fee_amount_gbp,
max(project_is_fixed_fee) as project_is_fixed_fee,
round(sum(total_hours_billed)) as total_hours_billed,
round(sum(round(consultant_cost*exchange_rate))) as total_consultant_cost_gbp,
round((total_revenue*exchange_rate)-sum(consultant_cost*exchange_rate)) as total_project_margin_gbp,
round((total_revenue*exchange_rate)/sum(total_hours_billed)) as effective_hourly_rate_gbp,
safe_divide((total_revenue*exchange_rate)-sum(consultant_cost*exchange_rate),(total_revenue*exchange_rate)) as project_margin,
round((sum(total_hours_billed)/project_budget_amount),2) as project_hours_vs_budget,
round(round(sum(round(consultant_cost*exchange_rate))) * round((sum(total_hours_billed)/project_budget_amount),2) - round(sum(round(consultant_cost*exchange_rate)))) as consulting_cost_budget_variance_gbp,
array_agg(struct(timesheet_billing_week,user_name,task_name,user_is_contractor,consultant_hours_billed,round(consultant_share_hours_billed_pct,2) as consultant_share_hours_billed_pct,
  round((total_revenue*exchange_rate)*consultant_share_hours_billed_pct) as consultant_share_total_revenue_gbp,
          round(consultant_cost*exchange_rate) as consultant_cost_gbp, round( (total_revenue*consultant_share_hours_billed_pct) - (consultant_cost*exchange_rate)) as consultant_contribution_gbp,
          round(safe_divide(((total_revenue*consultant_share_hours_billed_pct)-consultant_cost) , (total_revenue*consultant_share_hours_billed_pct)),2) as consultant_margin_pct ) ) consultant
from (
SELECT *,
  case when company_currency_code = 'USD' then 1.0
       when company_currency_code = 'EUR' then 1.0
       else 1 end as exchange_rate,
       COUNT(timesheet_billing_week) over (PARTITION BY timesheet_project_pk) as project_duration_weeks,
       ROUND(SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,timesheet_billing_week)) AS total_project_hours_billed,
       SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk, timesheet_task_pk,timesheet_billing_week ) as consultant_hours_billed,
       CASE WHEN user_is_contractor AND project_is_fixed_fee then ((project_fee_amount/(count(timesheet_billing_week) over (PARTITION BY timesheet_project_pk, user_pk, timesheet_task_pk,timesheet_billing_week)))/2) * SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk, timesheet_task_pk,timesheet_billing_week ) / ROUND(SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,user_pk))
            ELSE ((SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk, timesheet_task_pk,timesheet_billing_week )) * user_cost_rate) end as consultant_cost,
       SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_task_pk,timesheet_billing_week )/SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk) AS consultant_share_hours_billed_pct,
       ROUND(SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,user_pk)) AS total_consultant_project_hours_billed,
       SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk, timesheet_task_pk,timesheet_billing_week ) / ROUND(SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,user_pk)) as consultant_share_consultant_hours_billed_pct

FROM (
  SELECT
    c.company_pk,
    date_trunc(date(timesheet_billing_date),WEEK) as timesheet_billing_week,
    company_name,
    company_description,
    company_currency_code,
    t.timesheet_project_pk,
    project_name,
    t.timesheet_task_pk,
    a.task_name,
    project_code,
    project_delivery_start_ts,
    project_is_active,
    project_budget_by,
    project_fee_amount,
    project_budget_amount,
    project_is_fixed_fee,
    project_hourly_rate,
    project_cost_budget,
    t.user_pk,
    user_name,
    user_is_contractor,
    coalesce(u.user_cost_rate,25) as user_cost_rate,
    SUM(t.timesheet_hours_billed) AS total_hours_billed,
    coalesce(project_fee_amount,
      0)+SUM(timesheet_total_amount_billed) AS total_revenue
  FROM
    {{ ref('wh_timesheet_projects_dim') }} p
  JOIN
    {{ ref('wh_companies_dim') }} c
  ON
    p.company_pk = c.company_pk
  JOIN
    {{ ref('wh_timesheets_fact') }} t
  ON
    t.company_pk = c.company_pk
    AND t.company_pk = p.company_pk
    AND t.timesheet_project_pk = p.timesheet_project_pk
  JOIN
      {{ ref('wh_timesheet_tasks_dim') }} a
    ON
      t.timesheet_task_pk = a.timesheet_task_pk
  JOIN
    {{ ref('wh_users_dim') }} u
  ON
    t.user_pk = u.user_pk
  WHERE
    project_is_billable and timesheet_is_billable
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
  19,20,21,22)
    )
group by 1,2,3,4,5,6,7,8,9,10)
select *
from report
