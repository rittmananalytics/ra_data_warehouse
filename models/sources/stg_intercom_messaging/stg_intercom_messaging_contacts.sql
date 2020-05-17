{% if not var("enable_intercom_messaging_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
      {{ filter_stitch_table(var('stitch_contacts_table'),'id') }}
  ),
renamed as (
  SELECT
    id as contact_id,
    cast(null as string) as contact_first_name,
    cast(null as string) as contact_last_name,
    custom_attributes.job_title as job_title,
    email  as contact_email,
    cast(null as string) as contact_phone,
    cast(null as string) as contact_mobile_phone,
    cast(null as string) as contact_address,
    location_data.city_name as contact_city,
    location_data.region_name as contact_state,
    location_data.country_name as contact_country,
    location_data.postal_code contact_postcode_zip,
    cast(null as string)  contact_company,
    cast(null as string)  contact_website,
    cast(null as string) as contact_company_id,
    cast(null as string) as contact_owner_id,
    cast(null as string) as contact_lifecycle_stage,
    created_at as contact_created_date,
    updated_at as contact_last_modified_date
  FROM
    source)
select * from renamed
