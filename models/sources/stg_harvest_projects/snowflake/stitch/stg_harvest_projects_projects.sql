{{config(enabled = target.type == 'snowflake')}}
{% if var("projects_warehouse_timesheet_sources") %}
{% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_harvest_projects_stitch_projects_table'),unique_column='id') }}
),

renamed as (
select
       concat('{{ var('stg_harvest_projects_id-prefix') }}',p.ID)                  as timesheet_project_id,
       concat('{{ var('stg_harvest_projects_id-prefix') }}',p.CLIENT_ID)           as company_id,
       p.NAME                                   as project_name,
       p.CODE                                   as project_code,
       p.STARTS_ON                              as project_delivery_start_ts,
       p.ENDS_ON                                as project_delivery_end_ts,
       p.IS_ACTIVE                              as project_is_active,
       p.IS_BILLABLE                            as project_is_billable,
       p.HOURLY_RATE                            as project_hourly_rate,
       p.COST_BUDGET                            as project_cost_budget,
       p.IS_FIXED_FEE                           as project_is_fixed_fee,
       p.COST_BUDGET_INCLUDE_EXPENSES          as project_is_expenses_included_in_cost_budget,
       p.FEE                                    as project_fee_amount,
       p.BUDGET                                 as project_budget_amount,
       p.OVER_BUDGET_NOTIFICATION_PERCENTAGE    as project_over_budget_notification_pct,
       p.BUDGET_BY                              as project_budget_by
from source p)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
