{% if not var("enable_hubspot_crm_source")  %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{% if var("stg_hubspot_crm_etl") == 'fivetran' %}
WITH source as (
  select * from
  {{ target.database}}.{{ var('stg_hubspot_crm_fivetran_schema') }}.{{ var('stg_hubspot_crm_fivetran_contact_table') }}
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
       property_mobilephone as contact_mobile_phone,
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
       cast(null as boolean)         as user_is_contractor,
       cast(null as boolean) as user_is_staff,
       cast(null as int64)           as user_weekly_capacity,
       cast(null as int64)           as user_default_hourly_rate,
       cast(null as int64)           as user_cost_rate,
       false                          as user_is_active,
       property_createdate as contact_created_date,
       property_lastmodifieddate as contact_last_modified_date,
    from source
)
{% elif var("stg_hubspot_crm_etl") == 'stitch' %}
WITH source as (
  {{ filter_stitch_table(var('stg_hubspot_crm_stitch_schema'),var('stg_hubspot_crm_stitch_contacts_table'),'canonical_vid') }}

),
renamed as (
    select
       concat('{{ var('stg_hubspot_crm_id-prefix') }}',cast(canonical_vid as string)) as contact_id,
       properties.firstname.value as contact_first_name,
       properties.lastname.value as contact_last_name,
       coalesce(concat(properties.firstname.value,' ',properties.lastname.value),properties.email.value) as contact_name,
       properties.jobtitle.value contact_job_title,
       properties.email.value as contact_email,
       properties.phone.value as contact_phone,
       properties.mobilephone.value as contact_mobile_phone,
       properties.address.value contact_address,
       properties.city.value contact_city,
       properties.state.value contact_state,
       properties.country.value as contact_country,
       properties.zip.value contact_postcode_zip,
       properties.company.value contact_company,
       properties.website.value contact_website,
       concat('{{ var('stg_hubspot_crm_id-prefix') }}',cast(properties.associatedcompanyid.value as string)) as contact_company_id,
       concat('{{ var('stg_hubspot_crm_id-prefix') }}',cast(properties.hubspot_owner_id.value as string)) as contact_owner_id,
       properties.lifecyclestage.value as contact_lifecycle_stage,
       cast(null as boolean)         as user_is_contractor,
       cast(null as boolean) as user_is_staff,
       cast(null as int64)           as user_weekly_capacity,
       cast(null as int64)           as user_default_hourly_rate,
       cast(null as int64)           as user_cost_rate,
       false                          as user_is_active,
       properties.createdate.value as contact_created_date,
       properties.lastmodifieddate.value as contact_last_modified_date,
    from source
)
{% endif %}
select * from renamed
