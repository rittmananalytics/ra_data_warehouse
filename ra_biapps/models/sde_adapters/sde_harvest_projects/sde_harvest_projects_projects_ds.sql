{{
    config(
        materialized='table'
    )
}}
with harvest_projects as (
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_sdc_batched_at
    FROM
      {{ source('harvest', 'projects') }})
  WHERE
    latest_sdc_batched_at = _sdc_batched_at
)
select p.starts_on,
       p.is_active,
       p.id,
       p.cost_budget,
       p.name,
       p.is_fixed_fee,
       p.cost_budget_include_expenses,
       p.fee,
       p.budget,
       p.over_budget_notification_percentage,
       p.code,
       p.ends_on,
       p.budget_by,
       p.client_id,
       p.is_billable,
       p.hourly_rate
from {{ ref('harvest_projects')}} p
