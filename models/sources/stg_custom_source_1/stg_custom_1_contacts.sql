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
renamed AS (
  SELECT
  CONCAT('custom_1-',id) AS contact_id,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_first_name,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_last_name,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_name,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_job_title,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_email,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_phone,
  CAST(null AS {{ dbt_utils.type_string() }}) AS AS contact_phone_mobile,
  cCAST(null AS {{ dbt_utils.type_string() }}) AS contact_address,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_city,
  cCAST(null AS {{ dbt_utils.type_string() }}) AS contact_state,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_country,
  cCAST(null AS {{ dbt_utils.type_string() }}) AS contact_postcode_zip,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_company,
  cCAST(null AS {{ dbt_utils.type_string() }}) AS contact_website,
  CAST(null AS {{ dbt_utils.type_string() }}) AS AS contact_company_id,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_owner_id,
  CAST(null AS {{ dbt_utils.type_string() }}) AS contact_lifecycle_stage,
   CAST(null AS {{ dbt_utils.type_timestamp() }}) AS contact_created_date,
   CAST(null AS {{ dbt_utils.type_timestamp() }}) AS contact_last_modified_date
FROM
  source
)
SELECT
  *
FROM
  renamed
