{% if var("projects_warehouse_timesheet_sources") %}
{{
    config(
        unique_key='timesheet_projects_pk',
        alias='timesheet_projects_dim'
    )
}}


WITH timesheet_projects AS
  (
  SELECT {{ dbt_utils.star(from=ref('int_timesheet_projects')) }}
  FROM   {{ ref('int_timesheet_projects') }}
),
{% if target.type == 'bigquery' %}
  companies_dim as (
    SELECT {{ dbt_utils.star(from=ref('wh_companies_dim')) }}
    from {{ ref('wh_companies_dim') }}
  )
{% elif target.type == 'snowflake' %}
companies_dim as (
    SELECT c.company_pk, cf.value::string as company_id
    from {{ ref('wh_companies_dim') }} c,table(flatten(c.all_company_ids)) cf
)
{% else %}
    {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}
SELECT
  {{ dbt_utils.surrogate_key(['p.timesheet_project_id']) }} as timesheet_project_pk,
   c.company_pk,
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
   p.project_budget_by
FROM
   timesheet_projects p

{% if target.type == 'bigquery' %}
  JOIN companies_dim c
    ON p.company_id IN UNNEST(c.all_company_ids)
{% elif target.type == 'snowflake' %}
  JOIN companies_dim c
    ON p.company_id = c.company_id
 {% else %}
  {{ exceptions.raise_compiler_error(target.type ~" not supported in this project") }}
{% endif %}

{% else %}{{config(enabled=false)}}{% endif %}
