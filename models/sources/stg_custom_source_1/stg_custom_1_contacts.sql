{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  {{ filter_stitch_table(var('stitch_schema'),var('stitch_clients_table'),'id') }}

),
renamed as (
  SELECT
  concat('custom_1-',id) AS contact_id,
  {{ cast() }} AS contact_first_name,
  {{ cast() }} AS contact_last_name,
  {{ cast() }} AS contact_name,
  {{ cast() }} AS contact_job_title,
  {{ cast() }} AS contact_email,
  {{ cast() }} AS contact_phone,
  {{ cast() }} AS AS contact_phone_mobile,
  c{{ cast() }} AS contact_address,
  {{ cast() }} AS contact_city,
  c{{ cast() }} AS contact_state,
  {{ cast() }} AS contact_country,
  c{{ cast() }} AS contact_postcode_zip,
  {{ cast() }} AS contact_company,
  c{{ cast() }} AS contact_website,
  {{ cast() }} AS AS contact_company_id,
  {{ cast() }} AS contact_owner_id,
  {{ cast() }} AS contact_lifecycle_stage,
   {{ cast(datatype='timestamp') }} AS contact_created_date,
   {{ cast(datatype='timestamp') }} AS contact_last_modified_date
FROM
  source
)
SELECT
  *
FROM
  renamed
