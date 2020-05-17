{% if not var("enable_hubspot_crm_source") %}
  {{ config(
    enabled = false
  ) }}
{% endif %}

{% if var("etl") == 'fivetran' %}
  WITH source AS (

    SELECT
      *
    FROM
      {{ target.database}}.{{ var('fivetran_company_table') }}
  ),
  renamed AS (
    SELECT
      CONCAT(
        '{{ var('id-prefix') }}',
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
      property_createdate AS company_created_date,
      property_hs_lastmodifieddate company_last_modified_date
    FROM
      source
  )
  {% elif var("etl") == 'stitch' %}
  WITH source AS (
    {{ filter_stitch_table(var('stitch_companies_table'),'companyid') }}
  ),
  renamed AS (
    SELECT
      CONCAT(
        '{{ var('id-prefix') }}',
        companyid
      ) AS company_id,
      REPLACE(
        REPLACE(REPLACE(properties.name.value, 'Limited', ''), 'ltd', ''),
        ', Inc.',
        ''
      ) AS company_name,
      properties.address.value AS company_address,
      properties.address2.value AS company_address2,
      properties.city.value AS company_city,
      properties.state.value AS company_state,
      properties.country.value AS company_country,
      properties.zip.value AS company_zip,
      properties.phone.value AS company_phone,
      properties.website.value AS company_website,
      properties.industry.value AS company_industry,
      properties.linkedin_company_page.value AS company_linkedin_company_page,
      properties.linkedinbio.value AS company_linkedin_bio,
      properties.twitterhandle.value AS company_twitterhandle,
      properties.description.value AS company_description,
      CAST (
        NULL AS STRING
      ) AS company_finance_status,
      properties.createdate.value AS company_created_date,
      properties.hs_lastmodifieddate.value company_last_modified_date
    FROM
      source
  )
{% endif %}
SELECT
  *
FROM
  renamed
