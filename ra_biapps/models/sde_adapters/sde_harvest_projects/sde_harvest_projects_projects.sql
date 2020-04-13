{% if not var("enable_harvest_projects") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source('harvest_projects', 'projects') }})
  WHERE
    max_sdc_batched_at = _sdc_batched_at
),
renamed as (
select
       concat('harvest-',p.id)                                     as timesheet_project_id,
       p.name                                   as project_name,
       p.code                                   as project_code,
       p.starts_on                              as project_delivery_start_ts,
       p.ends_on                                as project_delivery_end_ts,
       p.is_active                              as project_is_active,
       p.is_billable                            as project_is_billable,
       p.hourly_rate                            as project_hourly_rate,
       p.cost_budget                            as project_cost_budget,
       p.is_fixed_fee                           as project_is_fixed_fee,
       p.cost_budget_include_expenses           as project_is_expenses_included_in_cost_budget,
       p.fee                                    as project_fee_amount,
       p.budget                                 as project_budget_amount,
       p.over_budget_notification_percentage    as project_over_budget_notification_pct,
       p.budget_by                              as project_budget_by,
       p.client_id                              as project_client_id
from source p)
SELECT
  *
FROM
  renamed
