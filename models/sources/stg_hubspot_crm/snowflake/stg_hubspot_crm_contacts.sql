{{config(enabled = target.type == 'snowflake')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_contact_sources") %}

{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
WITH source as (
  select * from
  {{ var('stg_hubspot_crm_fivetran_contacts_table') }}
),
renamed as (
    select
      concat('{{ var('stg_hubspot_crm_id-prefix') }}',canonical_vid::STRING) as contact_id,
      property_firstname:value::STRING as contact_first_name,
      property_lastname:value::STRING as contact_last_name,
      coalesce(concat(property_firstname:value::STRING,' ',property_lastname:value::STRING),property_email:value::STRING) as contact_name,
      property_jobtitle:value::STRING contact_job_title,
      property_email:value::STRING as contact_email,
      property_phone:value::STRING as contact_phone,
      property_address:value::STRING contact_address,
      property_city:value::STRING contact_city,
      property_state:value::STRING contact_state,
      property_country:value::STRING as contact_country,
      property_zip:value::STRING contact_postcode_zip,
      property_company:value::STRING contact_company,
      property_website:value::STRING contact_website,
      concat('{{ var('stg_hubspot_crm_id-prefix') }}',associated_company:"company-id":value::STRING) as contact_company_id,
      property_hubspot_owner_id:value::INT as contact_owner_id,
      property_lifecyclestage:value::STRING as contact_lifecycle_stage,
      cast(null as boolean)         as contact_is_contractor,
      cast(null as boolean) as contact_is_staff,
      cast(null as INT)           as contact_weekly_capacity,
      cast(null as INT)           as contact_default_hourly_rate,
      cast(null as INT)           as contact_cost_rate,
      false                          as contact_is_active,
      property_createdate:value::TIMESTAMP as contact_created_date,
      property_lastmodifieddate:value::TIMESTAMP as contact_last_modified_date
    from source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
WITH source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_contacts_table'),unique_column='canonical_vid') }}

),
renamed as (
    select
      canonical_vid::STRING as contact_id,
      property_firstname:value::STRING as contact_first_name,
      property_lastname:value::STRING as contact_last_name,
      coalesce(concat(property_firstname:value::STRING,' ',property_lastname:value::STRING),property_email:value::STRING) as contact_name,
      property_jobtitle:value::STRING contact_job_title,
      property_email:value::STRING as contact_email,
      property_phone:value::STRING as contact_phone,
      property_address:value::STRING contact_address,
      property_city:value::STRING contact_city,
      property_state:value::STRING contact_state,
      property_country:value::STRING as contact_country,
      property_zip:value::STRING contact_postcode_zip,
      property_company:value::STRING contact_company,
      property_website:value::STRING contact_website,
      associated_company:"company-id":value::STRING as contact_company_id,
      property_hubspot_owner_id:value::INT as contact_owner_id,
      property_lifecyclestage:value::STRING as contact_lifecycle_stage,
      cast(null as boolean)         as contact_is_contractor,
      cast(null as boolean) as contact_is_staff,
      cast(null as INT)           as contact_weekly_capacity,
      cast(null as INT)           as contact_default_hourly_rate,
      cast(null as INT)           as contact_cost_rate,
      false                          as contact_is_active,
      property_createdate:value::TIMESTAMP as contact_created_date,
      property_lastmodifieddate:value::TIMESTAMP as contact_last_modified_date
    from source
)
{% endif %}
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
