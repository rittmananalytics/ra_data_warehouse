{{config(enabled = target.type == 'bigquery')}}
{% if var("product_warehouse_usage_sources") %}
{% if 'bigquery_usage' in var("product_warehouse_usage_sources") %}


with source AS (
    SELECT
      *
    FROM
      {{ source('bigquery_usage_product_usage', 'cloudaudit_data_access') }}
  ),
 renamed as
 (
  SELECT
         CONCAT('{{ var('stg_bigquery_usage_id-prefix') }}',resource.labels.project_id)   AS company_id,
         resource.labels.project_id AS company_name,
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
         CAST(null AS {{ dbt_utils.type_string() }})     AS company_currency_code,
         CAST(null AS timestamp) AS company_created_date,
         CAST(null AS timestamp) AS company_last_modified_date
  FROM source
  {{dbt_utils.group_by(18) }} )
SELECT
 *
FROM
 renamed

 {% else %} {{config(enabled=false)}} {% endif %}
 {% else %} {{config(enabled=false)}} {% endif %}
