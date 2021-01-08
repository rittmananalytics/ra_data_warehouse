{{config(enabled = target.type == 'snowflake')}}
{% if var("projects_warehouse_timesheet_sources") %}
{% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_harvest_projects_stitch_tasks_table'),unique_column='id') }}
),
renamed as (
select
       concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(id as string))                  as task_id,
       name                as task_name,
       billable_by_default as task_billable_by_default,
       null                as task_default_hourly_rate,
       created_at          as task_created_at,
       updated_at          as task_updated_at,
       is_active           as task_is_active
from source)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
