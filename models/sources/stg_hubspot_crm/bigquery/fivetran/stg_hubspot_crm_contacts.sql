{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_contact_sources") %}

WITH source as (
  select * from
  {{ var('stg_hubspot_crm_fivetran_contacts_table') }}
),
renamed as (
    select
    CONCAT(
      concat('{{ var('stg_hubspot_crm_id-prefix') }}',cast(canonical_vid as string)) as contact_id,
       property_firstname as contact_first_name,
       property_lastname as contact_last_name,
       coalesce(concat(property_firstname,' ',property_lastname),property_email) as contact_name,
       property_jobtitle contact_job_title,
       property_email as contact_email,
       property_phone as contact_phone,
       property_address contact_address,
       property_city contact_city,
       property_state contact_state,
       property_country as contact_country,
       property_zip contact_postcode_zip,
       property_company contact_company,
       property_website contact_website,
       cast(property_associatedcompanyid as string) as contact_company_id,
       cast(property_hubspot_owner_id as string) as contact_owner_id,
       property_lifecyclestage as contact_lifecycle_stage,
       cast(null as boolean)         as contact_is_contractor,
       cast(null as boolean) as contact_is_staff,
       cast(null as int64)           as contact_weekly_capacity,
       cast(null as int64)           as contact_default_hourly_rate,
       cast(null as int64)           as contact_cost_rate,
       false                          as contact_is_active,
       property_createdate as contact_created_date,
       property_lastmodifieddate as contact_last_modified_date,
    from source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
