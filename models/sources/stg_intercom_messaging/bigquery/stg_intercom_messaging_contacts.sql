{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_contacts_sources") %}
{% if 'intercom_messaging' in var("crm_warehouse_contacts_sources") %}

WITH source AS (
      {{ filter_stitch_table(var('stg_intercom_messaging_stitch_schema'),var('stg_intercom_messaging_stitch_contacts_table'),'id') }}
  ),
renamed as (
  SELECT
    concat('{{ var('stg_intercom_messaging_id-prefix') }}',id) as contact_id,
    cast(null as string) as contact_first_name,
    cast(null as string) as contact_last_name,
    custom_attributes.job_title as job_title,
    email  as contact_email,
    cast(null as string) as contact_phone,
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
    cast(null as boolean)         as contact_is_contractor,
    cast(null as boolean) as contact_is_staff,
    cast(null as int64)           as contact_weekly_capacity,
    cast(null as int64)           as contact_default_hourly_rate,
    cast(null as int64)           as contact_cost_rate,
    false                          as contact_is_active,
    created_at as contact_created_date,
    updated_at as contact_last_modified_date
  FROM
    source)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
