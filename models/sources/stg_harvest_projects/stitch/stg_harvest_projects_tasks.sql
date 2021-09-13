{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("projects_warehouse_timesheet_sources") %}
{% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}

with source as (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'tasks'),unique_column='id') }}
),
renamed as (
select
       concat('{{ var('stg_harvest_projects_id-prefix') }}',cast(id as {{ dbt_utils.type_string() }}))                  as task_id,
       name                as task_name,
       billable_by_default as task_billable_by_default,
       default_hourly_rate as task_default_hourly_rate,
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
  {% else %} {{config(enabled=false)}} {% endif %}
