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
project_name,
project_duration_months,
task_name,
project_code,
max(project_delivery_start_ts) as project_delivery_start_ts ,
max(project_is_active) as project_is_active,
max(project_budget_by) as project_budget_by,
round(safe_divide(round(max(project_fee_amount)*exchange_rate),project_duration_months)) as project_fee_amount,
round(safe_divide(round(max(project_budget_amount)*exchange_rate),project_duration_months)) as project_budget_amount,
max(project_is_fixed_fee) as project_is_fixed_fee,
round(sum(total_hours_billed)) as total_hours_billed,
round(sum(round(consultant_share_revenue))*exchange_rate) as total_revenue,
round(sum(round(consultant_cost))*exchange_rate) as total_consultant_cost,
round(sum(round(consultant_contribution))*exchange_rate) as total_consultant_contribution,
round(sum(round(consultant_share_revenue))/sum(total_hours_billed)*exchange_rate,0) as effective_hourly_rate,
round(safe_divide(sum(round(consultant_contribution)),sum(round(consultant_share_revenue))),2) as project_margin_pct,
array_agg(struct(timesheet_billing_month,user_name,task_name,user_is_contractor,consultant_hours_billed,consultant_share_hours_billed,round(consultant_share_revenue) as consultant_share_local_revenue,round(consultant_cost) as consultant_local_cost,round(consultant_contribution) as consultant_local_contribution, round(consultant_effective_hourly_rate) as consultant_effective_local_hourly_rate, round(consultant_margin_pct,2) as consultant_margin_pct ) ) consultant
from (
SELECT *,
  case when company_currency_code = 'USD' then 1
       when company_currency_code = 'EUR' then 1
       else 1 end as exchange_rate,
       count(timesheet_billing_month) over (PARTITION BY timesheet_project_pk, timesheet_billing_month) as project_duration_months,
  round(SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,timesheet_billing_month)) AS total_project_hours_billed,
  round(SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) as consultant_hours_billed,
  case when user_is_contractor and project_is_fixed_fee then ((project_fee_amount/(count(timesheet_billing_month) over (PARTITION BY timesheet_project_pk, timesheet_billing_month)))/2)
       when user_is_contractor and not project_is_fixed_fee then (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *50
       when user_name = 'Mark Rittman' then (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *50
       else (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *25 end as consultant_cost,
  round(total_hours_billed/SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,timesheet_billing_month),2) AS consultant_share_hours_billed,

  (round((total_revenue/count(timesheet_billing_month) over (PARTITION BY timesheet_project_pk, timesheet_billing_month)) * (total_hours_billed/SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,timesheet_billing_month)),2)) as consultant_share_revenue,

  (round((total_revenue/count(timesheet_billing_month) over (PARTITION BY timesheet_project_pk, timesheet_billing_month)) * (total_hours_billed/SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,timesheet_billing_month)),2)) -

  (case when user_is_contractor and project_is_fixed_fee then ((project_fee_amount/(count(timesheet_billing_month) over (PARTITION BY timesheet_project_pk, timesheet_billing_month)))/2)
       when user_is_contractor and not project_is_fixed_fee then (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *50
       when user_name = 'Mark Rittman' then (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *50
       else (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *25 end) as consultant_contribution,


       (round((total_revenue/count(timesheet_billing_month) over (PARTITION BY timesheet_project_pk, timesheet_billing_month)) * (total_hours_billed/SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,timesheet_billing_month)),2)) /
       SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month ) as consultant_effective_hourly_rate,
       safe_divide(((round(total_revenue * (total_hours_billed/SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,timesheet_billing_month)),2)) -
  (case when user_is_contractor and project_is_fixed_fee then project_fee_amount/2
       when user_is_contractor and not project_is_fixed_fee then (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *50
       when user_name = 'Mark Rittman' then (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *50
       else (SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk, user_pk,timesheet_billing_month )) *25 end)) , (coalesce((round(total_revenue * (total_hours_billed/SUM(total_hours_billed) OVER (PARTITION BY timesheet_project_pk,timesheet_billing_month)),2)),0))) as consultant_margin_pct


FROM (
  SELECT
    c.company_pk,
    date_trunc(date(timesheet_billing_date),MONTH) as timesheet_billing_month,
    company_name,
    company_description,
    company_currency_code,
    t.timesheet_project_pk,
    project_name,
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
  19,20)
    )
group by 1,2,3,4,5,6,7,8,9)
select *
from report
