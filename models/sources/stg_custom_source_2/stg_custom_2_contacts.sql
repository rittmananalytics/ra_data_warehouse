{% if not var("enable_custom_source_2") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    select *
    from
    {{ source('custom_source_2','s_accounts' ) }}
),
renamed as (
  SELECT
  concat('custom_2-',id) AS contact_id,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_first_name,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_last_name,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_name,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_job_title,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_email,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_phone,
  cast(null as {{ dbt_utils.type_string() }}) AS AS contact_phone_mobile,
  ccast(null as {{ dbt_utils.type_string() }}) AS contact_address,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_city,
  ccast(null as {{ dbt_utils.type_string() }}) AS contact_state,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_country,
  ccast(null as {{ dbt_utils.type_string() }}) AS contact_postcode_zip,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_company,
  ccast(null as {{ dbt_utils.type_string() }}) AS contact_website,
  cast(null as {{ dbt_utils.type_string() }}) AS AS contact_company_id,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_owner_id,
  cast(null as {{ dbt_utils.type_string() }}) AS contact_lifecycle_stage,
   cast(null as {{ dbt_utils.type_timestamp() }}) AS contact_created_date,
   cast(null as {{ dbt_utils.type_timestamp() }}) AS contact_last_modified_date
FROM
  source
)
SELECT
  *
FROM
  renamed
