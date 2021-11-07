{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("crm_warehouse_contact_sources") %}
{% if 'jira_projects' in var("crm_warehouse_contact_sources") %}

with source as (
  {{ filter_stitch_relation(relation=source('stitch_jira_projects','users'),unique_column='accountid') }}
),
renamed as
 (
  SELECT
    concat('{{ var('stg_jira_projects_id-prefix') }}',accountid) AS contact_id,
    split(displayname,' ')[safe_offset(0)] AS contact_first_name,
    split(displayname,' ')[safe_offset(1)] AS contact_last_name,
    displayname AS contact_name,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_job_title,
    emailaddress AS contact_email,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_phone,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_address,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_city,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_state,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_country,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_postcode_zip,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_company,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_website,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_company_id,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_owner_id,
    cast(null as {{ dbt_utils.type_string() }}) AS contact_lifecycle_stage,
    cast(null as {{ dbt_utils.type_boolean() }})         as contact_is_contractor,
    case when emailaddress like '%@{{ var('stg_jira_projects_staff_email_domain') }}%' then true else false end as contact_is_staff,
     cast(null as {{ dbt_utils.type_int() }})           as contact_weekly_capacity,
     cast(null as {{ dbt_utils.type_int() }})           as contact_default_hourly_rate,
     cast(null as {{ dbt_utils.type_int() }})           as contact_cost_rate,
    active                        as contact_is_active,
     cast(null as {{ dbt_utils.type_timestamp() }}) AS contact_created_date,
     cast(null as {{ dbt_utils.type_timestamp() }}) AS contact_last_modified_date
  FROM source
    WHERE concat('{{ var('stg_jira_projects_id-prefix') }}',accountid)  NOT LIKE '%addon%'
  UNION ALL
    SELECT
      concat('{{ var('stg_jira_projects_id-prefix') }}',-999) AS contact_id,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_first_name,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_last_name,
      'Unassigned'  AS contact_name,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_job_title,
      'unassigned@example.com' AS contact_email,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_phone,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_address,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_city,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_state,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_country,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_postcode_zip,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_company,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_website,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_company_id,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_owner_id,
      cast(null as {{ dbt_utils.type_string() }}) AS contact_lifecycle_stage,
      cast(null as {{ dbt_utils.type_boolean() }})         as contact_is_contractor,
      false as contact_is_staff,
       cast(null as {{ dbt_utils.type_int() }})           as contact_weekly_capacity,
       cast(null as {{ dbt_utils.type_int() }})           as contact_default_hourly_rate,
       cast(null as {{ dbt_utils.type_int() }})           as contact_cost_rate,
      false                          as contact_is_active,
       cast(null as {{ dbt_utils.type_timestamp() }}) AS contact_created_date,
       cast(null as {{ dbt_utils.type_timestamp() }}) AS contact_last_modified_date
    )
    SELECT
     *
    FROM
     renamed

     {% else %} {{config(enabled=false)}} {% endif %}
     {% else %} {{config(enabled=false)}} {% endif %}
     {% else %} {{config(enabled=false)}} {% endif %}
