{{config(enabled = target.type == 'bigquery')}}
{% if var("projects_warehouse_delivery_sources") %}
{% if 'asana_projects' in var("projects_warehouse_delivery_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_asana_projects_stitch_projects_table'),unique_column='gid') }}
),

renamed AS (
  SELECT
  CONCAT('{{ var('stg_asana_projects_id-prefix') }}',gid) AS project_id,
  CONCAT('{{ var('stg_asana_projects_id-prefix') }}',owner.gid) AS lead_user_id,
  CONCAT('{{ var('stg_asana_projects_id-prefix') }}',workspace.gid) AS company_id,
  name AS project_name,
  current_status AS project_status,
  notes AS project_notes,
  CAST(null AS {{ dbt_utils.type_string() }}) AS project_type,
  CAST(null AS {{ dbt_utils.type_string() }}) AS project_category_description,
  CAST(null AS {{ dbt_utils.type_string() }}) AS project_category_name,
  created_at AS project_created_at_ts,
  modified_at AS project_modified_at_ts,
  FROM
    source


)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
