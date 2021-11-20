{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("projects_warehouse_timesheet_sources") %}
{% if 'harvest_projects' in var("projects_warehouse_timesheet_sources") %}

with source AS (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'tasks'),unique_column='id') }}
),
renamed AS (
SELECT
       CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',CAST(id AS {{ dbt_utils.type_string() }}))                  AS task_id,
       name                AS task_name,
       billable_by_default AS task_billable_by_default,
       default_hourly_rate AS task_default_hourly_rate,
       created_at          AS task_created_at,
       updated_at          AS task_updated_at,
       is_active           AS task_is_active
FROM source)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
