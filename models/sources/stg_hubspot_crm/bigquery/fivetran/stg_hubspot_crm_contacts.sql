{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_contact_sources") %}

WITH source AS (
  SELECT * from
  {{ source('fivetran_hubspot_crm','contacts') }}
),
renamed AS (
    SELECT
    CONCAT(
      CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',CAST(canonical_vid AS string)) AS contact_id,
       property_firstname AS contact_first_name,
       property_lastname AS contact_last_name,
       coalesce(CONCAT(property_firstname,' ',property_lastname),property_email) AS contact_name,
       property_jobtitle contact_job_title,
       property_email AS contact_email,
       property_phone AS contact_phone,
       property_address contact_address,
       property_city contact_city,
       property_state contact_state,
       property_country AS contact_country,
       property_zip contact_postcode_zip,
       property_company contact_company,
       property_website contact_website,
       CAST(property_associatedcompanyid AS string) AS contact_company_id,
       CAST(property_hubspot_owner_id AS string) AS contact_owner_id,
       property_lifecyclestage AS contact_lifecycle_stage,
       CAST(null AS {{ dbt_utils.type_boolean() }})         AS contact_is_contractor,
       CAST(null AS {{ dbt_utils.type_boolean() }}) AS contact_is_staff,
        CAST(null AS {{ dbt_utils.type_int() }})           AS contact_weekly_capacity,
        CAST(null AS {{ dbt_utils.type_int() }})           AS contact_default_hourly_rate,
        CAST(null AS {{ dbt_utils.type_int() }})           AS contact_cost_rate,
       false                          AS contact_is_active,
       property_createdate AS contact_created_date,
       property_lastmodifieddate AS contact_last_modified_date,
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
