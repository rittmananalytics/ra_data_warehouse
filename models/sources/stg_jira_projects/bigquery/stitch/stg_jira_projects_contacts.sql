{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'jira_projects' in var("crm_warehouse_contact_sources") %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_jira_projects_stitch_users_table'),unique_column='accountid') }}
),
renamed as
 (
  SELECT
    concat('{{ var('stg_jira_projects_id-prefix') }}',accountid) AS contact_id,
    split(displayname,' ')[safe_offset(0)] AS contact_first_name,
    split(displayname,' ')[safe_offset(1)] AS contact_last_name,
    displayname AS contact_name,
    {{ cast() }} AS contact_job_title,
    emailaddress AS contact_email,
    {{ cast() }} AS contact_phone,
    {{ cast() }} AS contact_address,
    {{ cast() }} AS contact_city,
    {{ cast() }} AS contact_state,
    {{ cast() }} AS contact_country,
    {{ cast() }} AS contact_postcode_zip,
    {{ cast() }} AS contact_company,
    {{ cast() }} AS contact_website,
    {{ cast() }} AS contact_company_id,
    {{ cast() }} AS contact_owner_id,
    {{ cast() }} AS contact_lifecycle_stage,
    cast(null as boolean)         as contact_is_contractor,
    case when emailaddress like '%@{{ var('stg_jira_projects_staff_email_domain') }}%' then true else false end as contact_is_staff,
     {{ cast(datatype='integer') }}           as contact_weekly_capacity,
     {{ cast(datatype='integer') }}           as contact_default_hourly_rate,
     {{ cast(datatype='integer') }}           as contact_cost_rate,
    active                        as contact_is_active,
     {{ cast(datatype='timestamp') }} AS contact_created_date,
     {{ cast(datatype='timestamp') }} AS contact_last_modified_date
  FROM source
    WHERE concat('{{ var('stg_jira_projects_id-prefix') }}',accountid)  NOT LIKE '%addon%'
  UNION ALL
    SELECT
      concat('{{ var('stg_jira_projects_id-prefix') }}',-999) AS contact_id,
      {{ cast() }} AS contact_first_name,
      {{ cast() }} AS contact_last_name,
      'Unassigned'  AS contact_name,
      {{ cast() }} AS contact_job_title,
      'unassigned@example.com' AS contact_email,
      {{ cast() }} AS contact_phone,
      {{ cast() }} AS contact_address,
      {{ cast() }} AS contact_city,
      {{ cast() }} AS contact_state,
      {{ cast() }} AS contact_country,
      {{ cast() }} AS contact_postcode_zip,
      {{ cast() }} AS contact_company,
      {{ cast() }} AS contact_website,
      {{ cast() }} AS contact_company_id,
      {{ cast() }} AS contact_owner_id,
      {{ cast() }} AS contact_lifecycle_stage,
      cast(null as boolean)         as contact_is_contractor,
      false as contact_is_staff,
       {{ cast(datatype='integer') }}           as contact_weekly_capacity,
       {{ cast(datatype='integer') }}           as contact_default_hourly_rate,
       {{ cast(datatype='integer') }}           as contact_cost_rate,
      false                          as contact_is_active,
       {{ cast(datatype='timestamp') }} AS contact_created_date,
       {{ cast(datatype='timestamp') }} AS contact_last_modified_date
    )
    SELECT
     *
    FROM
     renamed

     {% else %} {{config(enabled=false)}} {% endif %}
     {% else %} {{config(enabled=false)}} {% endif %}
