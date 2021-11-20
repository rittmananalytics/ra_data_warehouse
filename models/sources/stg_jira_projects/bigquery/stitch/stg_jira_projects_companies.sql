{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("crm_warehouse_company_sources") %}
{% if 'jira_projects' in var("crm_warehouse_company_sources") %}

  WITH source AS (
      {{ filter_stitch_relation(relation=source('stitch_jira_projects','projects'),unique_column='id') }}
  ),
renamed AS (
SELECT * FROM (
SELECT
CONCAT('{{ var('stg_jira_projects_id-prefix') }}',replace(name,' ','_')) AS company_id,
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
    CAST(null AS {{ dbt_utils.type_string() }})     AS company_currency_code,
    CAST(null AS timestamp) AS company_created_date,
    CAST(null AS timestamp) AS company_last_modified_date
    FROM source )
    {{ dbt_utils.group_by(n=19) }})
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
