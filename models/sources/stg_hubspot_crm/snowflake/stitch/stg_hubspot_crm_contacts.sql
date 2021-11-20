{{config(enabled = target.type == 'snowflake')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_contact_sources") %}

{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
WITH source AS (
  SELECT * from
  {{ var('stg_hubspot_crm_fivetran_contacts_table') }}
),
renamed AS (
    SELECT
      CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',canonical_vid::STRING) AS contact_id,
      property_firstname:value::STRING AS contact_first_name,
      property_lastname:value::STRING AS contact_last_name,
      coalesce(CONCAT(property_firstname:value::STRING,' ',property_lastname:value::STRING),property_email:value::STRING) AS contact_name,
      property_jobtitle:value::STRING contact_job_title,
      property_email:value::STRING AS contact_email,
      property_phone:value::STRING AS contact_phone,
      property_address:value::STRING contact_address,
      property_city:value::STRING contact_city,
      property_state:value::STRING contact_state,
      property_country:value::STRING AS contact_country,
      property_zip:value::STRING contact_postcode_zip,
      property_company:value::STRING contact_company,
      property_website:value::STRING contact_website,
      CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',associated_company:"company-id":value::STRING) AS contact_company_id,
      property_hubspot_owner_id:value::INT AS contact_owner_id,
      property_lifecyclestage:value::STRING AS contact_lifecycle_stage,
      CAST(null AS {{ dbt_utils.type_boolean() }})         AS contact_is_contractor,
      CAST(null AS {{ dbt_utils.type_boolean() }}) AS contact_is_staff,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_weekly_capacity,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_default_hourly_rate,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_cost_rate,
      false                          AS contact_is_active,
      property_createdate:value::TIMESTAMP AS contact_created_date,
      property_lastmodifieddate:value::TIMESTAMP AS contact_last_modified_date
    FROM source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
WITH source AS (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_contacts_table'),unique_column='canonical_vid') }}

),
renamed AS (
    SELECT
      canonical_vid::STRING AS contact_id,
      property_firstname:value::STRING AS contact_first_name,
      property_lastname:value::STRING AS contact_last_name,
      coalesce(CONCAT(property_firstname:value::STRING,' ',property_lastname:value::STRING),property_email:value::STRING) AS contact_name,
      property_jobtitle:value::STRING contact_job_title,
      property_email:value::STRING AS contact_email,
      property_phone:value::STRING AS contact_phone,
      property_address:value::STRING contact_address,
      property_city:value::STRING contact_city,
      property_state:value::STRING contact_state,
      property_country:value::STRING AS contact_country,
      property_zip:value::STRING contact_postcode_zip,
      property_company:value::STRING contact_company,
      property_website:value::STRING contact_website,
      associated_company:"company-id":value::STRING AS contact_company_id,
      property_hubspot_owner_id:value::INT AS contact_owner_id,
      property_lifecyclestage:value::STRING AS contact_lifecycle_stage,
      CAST(null AS {{ dbt_utils.type_boolean() }})         AS contact_is_contractor,
      CAST(null AS {{ dbt_utils.type_boolean() }}) AS contact_is_staff,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_weekly_capacity,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_default_hourly_rate,
       CAST(null AS {{ dbt_utils.type_int() }})           AS contact_cost_rate,
      false                          AS contact_is_active,
      property_createdate:value::TIMESTAMP AS contact_created_date,
      property_lastmodifieddate:value::TIMESTAMP AS contact_last_modified_date
    FROM source
)
{% endif %}
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
