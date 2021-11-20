{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("crm_warehouse_contact_sources") %}
{% if 'jira_projects' in var("crm_warehouse_contact_sources") %}

with source AS (
  {{ filter_stitch_relation(relation=source('stitch_jira_projects','users'),unique_column='accountid') }}
),
renamed as
 (
  SELECT
    CONCAT('{{ var('stg_jira_projects_id-prefix') }}',accountid) AS contact_id,
    {{ dbt_utils.split_part('displayname',' ','1') }} AS contact_first_name,
    {{ dbt_utils.split_part('displayname',' ','1') }} AS contact_last_name,
    displayname AS contact_name,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_job_title,
    emailaddress AS contact_email,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_phone,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_address,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_city,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_state,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_country,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_postcode_zip,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_company,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_website,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_company_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_owner_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS contact_lifecycle_stage,
    CAST(null AS {{ dbt_utils.type_boolean() }})         AS contact_is_contractor,
    case when emailaddress like '%@{{ var('stg_jira_projects_staff_email_domain') }}%' then true else false end AS contact_is_staff,
     CAST(null AS {{ dbt_utils.type_int() }})           AS contact_weekly_capacity,
     CAST(null AS {{ dbt_utils.type_int() }})           AS contact_default_hourly_rate,
     CAST(null AS {{ dbt_utils.type_int() }})           AS contact_cost_rate,
    active                        AS contact_is_active,
     CAST(null AS {{ dbt_utils.type_timestamp() }}) AS contact_created_date,
     CAST(null AS {{ dbt_utils.type_timestamp() }}) AS contact_last_modified_date
  FROM source
    WHERE CONCAT('{{ var('stg_jira_projects_id-prefix') }}',accountid)  NOT LIKE '%addon%'
  UNION ALL
    SELECT
      CONCAT('{{ var('stg_jira_projects_id-prefix') }}',-999) AS contact_id,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_first_name,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_last_name,
      'Unassigned'  AS contact_name,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_job_title,
      'unassigned@example.com' AS contact_email,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_phone,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_address,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_city,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_state,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_country,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_postcode_zip,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_company,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_website,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_company_id,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_owner_id,
      CAST(null AS {{ dbt_utils.type_string() }}) AS contact_lifecycle_stage,
      CAST(null AS {{ dbt_utils.type_boolean() }})         AS contact_is_contractor,
      false AS contact_is_staff,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_weekly_capacity,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_default_hourly_rate,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_cost_rate,
      false                          AS contact_is_active,
       CAST(null AS {{ dbt_utils.type_timestamp() }}) AS contact_created_date,
       CAST(null AS {{ dbt_utils.type_timestamp() }}) AS contact_last_modified_date
    )
    SELECT
     *
    FROM
     renamed

     {% else %} {{config(enabled=false)}} {% endif %}
     {% else %} {{config(enabled=false)}} {% endif %}
     {% else %} {{config(enabled=false)}} {% endif %}
