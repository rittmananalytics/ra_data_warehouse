{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("projects_warehouse_timesheet_sources") %}
{% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}

with source AS (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'projects'),unique_column='id') }}
),

renamed AS (
SELECT
       CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',p.id)                  AS timesheet_project_id,
       CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',p.client_id)           AS company_id,
       p.name                                   AS project_name,
       p.code                                   AS project_code,
       p.starts_on                              AS project_delivery_start_ts,
       p.ends_on                                AS project_delivery_end_ts,
       p.is_active                              AS project_is_active,
       p.is_billable                            AS project_is_billable,
       p.hourly_rate                            AS project_hourly_rate,
       p.cost_budget                            AS project_cost_budget,
       p.is_fixed_fee                           AS project_is_fixed_fee,
       p.cost_budget_include_expenses           AS project_is_expenses_included_in_cost_budget,
       p.fee                                    AS project_fee_amount,
       p.budget                                 AS project_budget_amount,
       p.over_budget_notification_percentage    AS project_over_budget_notification_pct,
       p.budget_by                              AS project_budget_by
FROM source p)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
