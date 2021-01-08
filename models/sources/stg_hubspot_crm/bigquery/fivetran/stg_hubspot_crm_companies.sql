{{config(enabled = target.type == 'bigquery')}}
{% if var("crm_warehouse_company_sources") %}
{% if 'hubspot_crm' in var("crm_warehouse_company_sources") %}

WITH source AS (

    SELECT
      *
    FROM
    {{ var('stg_hubspot_crm_fivetran_companies_table') }}
  ),
  renamed AS (
    SELECT
      CONCAT(
        '{{ var('stg_hubspot_crm_id-prefix') }}',
        id
      ) AS company_id,
      REPLACE(
        REPLACE(REPLACE(property_name, 'Limited', ''), 'ltd', ''),
        ', Inc.',
        ''
      ) AS company_name,
      property_address AS company_address,
      property_address_2 AS company_address2,
      property_city AS company_city,
      property_state AS company_state,
      property_country AS company_country,
      property_zip AS company_zip,
      property_phone AS company_phone,
      property_website AS company_website,
      property_industry AS company_industry,
      property_linkedin_company_page AS company_linkedin_company_page,
      property_linkedinbio AS company_linkedin_bio,
      property_twitterhandle AS company_twitterhandle,
      property_description AS company_description,
      CAST (
        NULL AS STRING
      ) AS company_finance_status,
      cast (null as string)     as company_currency_code,
      property_createdate AS company_created_date,
      property_hs_lastmodifieddate company_last_modified_date
    FROM
      source
  )

SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
