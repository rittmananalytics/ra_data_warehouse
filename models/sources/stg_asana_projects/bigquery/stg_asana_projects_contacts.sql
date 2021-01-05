{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'asana_projects' in var("crm_warehouse_contact_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_asana_projects_stitch_users_table'),unique_column='gid') }}
  ),

renamed as (
  SELECT
    concat('{{ var('stg_asana_projects_id-prefix') }}',gid)  AS contact_id,
    split(name,' ')[safe_offset(0)] AS contact_first_name,
    split(name,' ')[safe_offset(1)] AS contact_last_name,
    name AS contact_name,
    CAST(NULL AS STRING) AS contact_job_title,
    email AS contact_email,
    CAST(NULL AS STRING) AS contact_phone,
    CAST(NULL AS STRING) AS contact_address,
    CAST(NULL AS STRING) AS contact_city,
    CAST(NULL AS STRING) AS contact_state,
    CAST(NULL AS STRING) AS contact_country,
    CAST(NULL AS STRING) AS contact_postcode_zip,
    CAST(NULL AS STRING) AS contact_company,
    CAST(NULL AS STRING) AS contact_website,
    CAST(NULL AS STRING) AS contact_company_id,
    CAST(NULL AS STRING) AS contact_owner_id,
    CAST(NULL AS STRING) AS contact_lifecycle_stage,
    cast(null as boolean)         as contact_is_contractor,
    case when email like '%@{{ var('stg_asana_projects_staff_email_domain') }}%' then true else false end as contact_is_staff,
    cast(null as int64)           as contact_weekly_capacity,
    cast(null as int64)           as contact_default_hourly_rate,
    cast(null as int64)           as contact_cost_rate,
    true                          as contact_is_active,
    cast(null as timestamp) AS contact_created_date,
    cast(null as timestamp) AS contact_last_modified_date
  FROM source
  WHERE
    name NOT LIKE 'Private User'
  UNION ALL
    SELECT
      concat('{{ var('stg_asana_projects_id-prefix') }}',-999) AS contact_id,
      CAST(NULL AS STRING) AS contact_first_name,
      CAST(NULL AS STRING) AS contact_last_name,
      'Unassigned'  AS contact_name,
      CAST(NULL AS STRING) AS contact_job_title,
      'unassigned@example.com' AS contact_email,
      CAST(NULL AS STRING) AS contact_phone,
      CAST(NULL AS STRING) AS contact_address,
      CAST(NULL AS STRING) AS contact_city,
      CAST(NULL AS STRING) AS contact_state,
      CAST(NULL AS STRING) AS contact_country,
      CAST(NULL AS STRING) AS contact_postcode_zip,
      CAST(NULL AS STRING) AS contact_company,
      CAST(NULL AS STRING) AS contact_website,
      CAST(NULL AS STRING) AS contact_company_id,
      CAST(NULL AS STRING) AS contact_owner_id,
      CAST(NULL AS STRING) AS contact_lifecycle_stage,
      cast(null as boolean)         as contact_is_contractor,
      false as contact_is_staff,
      cast(null as int64)           as contact_weekly_capacity,
      cast(null as int64)           as contact_efault_hourly_rate,
      cast(null as int64)           as contact_cost_rate,
      false                          as contact__is_active,
      cast(null as timestamp) AS contact_created_date,
      cast(null as timestamp) AS contact_last_modified_date
    )
    SELECT
     *
    FROM
     renamed

     {% else %} {{config(enabled=false)}} {% endif %}
     {% else %} {{config(enabled=false)}} {% endif %}
