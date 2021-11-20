{{config(enabled = target.type == 'bigquery')}}
{% if not var("enable_gcp_billing_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source AS (
    SELECT
      *
    FROM
      {{ source('gcp_billing', 'gcp_billing_export') }}
  ),
 renamed as
 (
  SELECT
  SELECT
         CONCAT('gcp-',project.id  AS string)) AS company_id,
         name AS company_name,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_address,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_address2,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_city,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_state,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_country,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_zip,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_phone,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_website,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_industry,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_linkedin_company_page,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_linkedin_bio,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_twitterhandle,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_description,
         CAST(null AS {{ dbt_utils.type_string() }}) AS company_finance_status,
          CAST(null AS {{ dbt_utils.type_timestamp() }}) AS company_created_date,
          CAST(null AS {{ dbt_utils.type_timestamp() }}) AS company_last_modified_date
  FROM source
  GROUP BY 1)
SELECT
 *
FROM
 renamed
