{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_hubspot_crm_etl") == 'stitch')
   )
}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_contact_sources") %}


WITH source AS (
  {{ filter_stitch_relation(relation=source('stitch_hubspot_crm','contacts'),unique_column='canonical_vid') }}

),
renamed AS (
    SELECT
       CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',CAST(canonical_vid AS string)) AS contact_id,
       properties.firstname.value AS contact_first_name,
       properties.lastname.value AS contact_last_name,
       coalesce(CONCAT(properties.firstname.value,' ',properties.lastname.value),properties.email.value) AS contact_name,
       properties.jobtitle.value contact_job_title,
       properties.email.value AS contact_email,
       properties.phone.value AS contact_phone,
       properties.address.value contact_address,
       properties.city.value contact_city,
       properties.state.value contact_state,
       properties.country.value AS contact_country,
       properties.zip.value contact_postcode_zip,
       properties.company.value contact_company,
       properties.website.value contact_website,
       CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',CAST(properties.associatedcompanyid.value AS string)) AS contact_company_id,
       CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',CAST(properties.hubspot_owner_id.value AS string)) AS contact_owner_id,
       properties.lifecyclestage.value AS contact_lifecycle_stage,
       CAST(null AS {{ dbt_utils.type_boolean() }})         AS contact_is_contractor,
       CAST(null AS {{ dbt_utils.type_boolean() }}) AS contact_is_staff,
        CAST(null AS {{ dbt_utils.type_int() }})           AS contact_weekly_capacity,
        CAST(null AS {{ dbt_utils.type_int() }})           AS contact_default_hourly_rate,
        CAST(null AS {{ dbt_utils.type_int() }})           AS contact_cost_rate,
       false                          AS contact_is_active,
       properties.createdate.value AS contact_created_date,
       properties.lastmodifieddate.value AS contact_last_modified_date,
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
