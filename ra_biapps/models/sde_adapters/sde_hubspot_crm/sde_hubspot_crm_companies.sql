{% if not var("enable_hubspot_crm") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source as (

  SELECT * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
  (
    SELECT *,
           MAX(_sdc_batched_at) OVER (PARTITION BY companyid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM {{ source('hubspot_crm', 'companies') }}
  )
  WHERE _sdc_batched_at = max_sdc_batched_at

),

renamed as (
    select
      concat('hubspot-',companyid) AS company_id,
      replace(replace(replace(properties.name.value,'Limited',''),'ltd',''),', Inc.','') AS company_name,
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
      cast (null as string) as company_finance_status,
      properties.createdate.value AS company_created_date,
      properties.hs_lastmodifieddate.value company_last_modified_date
    from source
)
SELECT
  *
FROM
  renamed
