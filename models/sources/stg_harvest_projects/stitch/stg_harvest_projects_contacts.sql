{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("crm_warehouse_contact_sources") %}
{% if 'harvest_projects' in var("crm_warehouse_contact_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'contacts'),unique_column='id') }}
),
renamed AS (
  WITH
  CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',CAST(id AS {{ dbt_utils.type_string() }}))         AS contact_id,
  first_name                                                                                              AS contact_first_name,
  last_name                                                                                               AS contact_last_name,
  CASE
    WHEN CONCAT(first_name,' ',last_name) = ' '
      THEN email ELSE CONCAT(first_name,' ',last_name)
    END                                                                                                   AS contact_name,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_job_title,
  email AS contact_email,
  replace(replace(replace(replace(telephone,'+','00'),' ',''),')',''),'(','')                             AS contact_phone,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_address,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_city,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_state,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_country,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_postcode_zip,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_company,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_website,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_company_id,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_owner_id,
  CAST(null AS {{ dbt_utils.type_string() }})                                                             AS contact_lifecycle_stage,
  is_contractor                                                                                           AS contact_is_contractor,
  true                                                                                                    AS contact_is_staff,
  weekly_capacity                                                                                         AS contact_weekly_capacity,
  default_hourly_rate                                                                                     AS contact_default_hourly_rate,
  cost_rate                                                                                               AS contact_cost_rate,
  is_active                                                                                               AS contact_is_active,
  MIN(updated_at) OVER (PARTITION BYid order by updated_at)                                               AS contact_created_date,
  updated_at                                                                                              AS contact_last_modified_date
FROM
  source
)
WITH
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
