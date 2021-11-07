{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("projects_warehouse_timesheet_sources") %}
{% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}

with t_harvest_time_entries as (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'timesheets'),unique_column='id') }}
),
t_harvest_projects as (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'projects'),unique_column='id') }}
),
t_harvest_users_project_tasks as (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'user_project_tasks'),unique_column='project_task_id') }}
  ),
t_harvest_project_tasks as (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'project_tasks'),unique_column='id') }}
  ),
t_harvest_tasks as (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'tasks'),unique_column='id') }}
  ),
t_harvest_users as (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'users'),unique_column='id') }}
),
renamed as (
SELECT
  concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(t.client_id as string))               as company_id,
  concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(t.id as string))      as timesheet_id,
  concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(t.user_id as string))  as timesheet_users_id,
  concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(t.project_id as string))             as timesheet_project_id,
  concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(t.task_assignment_id as string))   as timesheet_task_assignment_id,
  concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(coalesce(ht.id,-999) as string)) as timesheet_task_id,
  concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(t.invoice_id as string)) as timesheet_invoice_id,
  t.spent_date              as timesheet_billing_date,
  t.hours                   as timesheet_hours_billed,
  case when t.is_billed then t.billable_rate * t.hours else 0 end as timesheet_total_amount_billed,
  t.billable                as timesheet_is_billable,
  t.is_billed               as timesheet_has_been_billed,
  t.is_locked               as timesheet_has_been_locked,
  t.billable_rate           as timesheet_billable_hourly_rate_amount,
  t.cost_rate               as timesheet_billable_hourly_cost_amount,
  t.notes                   as timesheet_notes
FROM
  t_harvest_time_entries t
  join t_harvest_projects p on t.project_id = p.id
  left outer join t_harvest_users_project_tasks upt on t.task_assignment_id = upt.project_task_id and upt.user_id = t.user_id
  left outer join t_harvest_project_tasks pt on upt.project_task_id = pt.id
  left outer join t_harvest_tasks ht on pt.task_id = ht.id
  join t_harvest_users u on t.user_id = u.id
  {{ dbt_utils.group_by(n=16) }})
SELECT
    *
  FROM
    renamed

    {% else %} {{config(enabled=false)}} {% endif %}
    {% else %} {{config(enabled=false)}} {% endif %}
    {% else %} {{config(enabled=false)}} {% endif %}
