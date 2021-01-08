{{config
  (enabled =
      (target.type == 'bigquery' and var("stg_hubspot_crm_etl") == 'stitch')
   )
}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_contact_sources") %}


WITH source as (
  {{ filter_stitch_relation(relation=var('stg_hubspot_crm_stitch_contacts_table'),unique_column='canonical_vid') }}

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
       cast(null as boolean)         as contact_is_contractor,
       cast(null as boolean) as contact_is_staff,
       cast(null as int64)           as contact_weekly_capacity,
       cast(null as int64)           as contact_default_hourly_rate,
       cast(null as int64)           as contact_cost_rate,
       false                          as contact_is_active,
       properties.createdate.value as contact_created_date,
       properties.lastmodifieddate.value as contact_last_modified_date,
    from source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
