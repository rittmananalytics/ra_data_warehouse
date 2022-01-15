{% if target.type == 'bigquery' %}
{% if var("crm_warehouse_company_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_company_sources") %}
{% if var("stg_hubspot_crm_etl") == 'stitch' %}


  WITH source AS (
    {{ filter_stitch_relation(relation=source('stitch_hubspot_crm','companies'),unique_column='companyid') }}
  ),
  renamed AS (
    SELECT
      CONCAT('{{ var('stg_hubspot_crm_id-prefix') }}',companyid) AS company_id,
      REPLACE(REPLACE(REPLACE(properties.name.value, 'Limited', ''), 'ltd', ''),', Inc.','') AS company_name,
      properties.address.value AS                   company_address,
      properties.address2.value AS                  company_address2,
      properties.city.value AS                      company_city,
      properties.state.value AS                     company_state,
      properties.country.value AS                   company_country,
      properties.zip.value AS                       company_zip,
      properties.phone.value AS                     company_phone,
      properties.website.value AS                   company_website,
      properties.industry.value AS                  company_industry,
      properties.linkedin_company_page.value AS     company_linkedin_company_page,
      properties.linkedinbio.value AS               company_linkedin_bio,
      properties.twitterhandle.value AS             company_twitterhandle,
      properties.description.value AS               company_description,
      cast (null as {{ dbt_utils.type_string() }}) AS                      company_finance_status,
      cast (null as {{ dbt_utils.type_string() }})      as                 company_currency_code,
      properties.createdate.value AS                company_created_date,
      properties.hs_lastmodifieddate.value          company_last_modified_date
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
  {% else %} {{config(enabled=false)}} {% endif %}
