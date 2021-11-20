{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("crm_warehouse_company_sources") %}
{% if 'harvest_projects' in var("crm_warehouse_company_sources") %}

WITH source AS (
  {{ filter_stitch_relation(relation=source('stitch_harvest_projects', 'companies'),unique_column='id') }}
),
renamed AS (
  SELECT
    CONCAT('{{ var('stg_harvest_projects_id-prefix') }}',id) AS company_id,
    replace(replace(replace(name,'Limited',''),'ltd',''),', Inc.','') AS company_name,
    address AS company_address,
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
    currency              AS company_currency_code,
    created_at AS company_created_date,
    updated_at AS company_last_modified_date
FROM
  source
)
SELECT
  *
FROM
  renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
