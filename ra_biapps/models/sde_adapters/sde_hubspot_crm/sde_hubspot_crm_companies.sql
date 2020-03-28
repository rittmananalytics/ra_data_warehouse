
WITH hubspot_companies as (

  SELECT * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
  (
    SELECT *,
           MAX(_sdc_batched_at) OVER (PARTITION BY companyid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM {{ source('hubspot_crm', 'companies') }}
  )
  WHERE _sdc_batched_at = max_sdc_batched_at

),

companies_ds as (

    select
    'hubspot_crm' as source,

      companyid AS company_id,
      properties.name.value AS company_name,
      properties.description.value AS company_description,

      properties.linkedin_company_page.value AS company_linkedin_company_page,
      properties.twitterhandle.value AS company_twitterhandle,
      properties.address.value AS company_address,
      properties.address2.value AS company_address2,
      properties.city.value AS company_city,
      properties.country.value AS company_country,
      properties.website.value AS company_website,
      properties.hubspot_owner_id.value AS company_contact_owner_id,
      properties.industry.value AS company_industry,
      properties.linkedinbio.value AS company_linkedin_bio,
      properties.domain.value AS company_domain,
      properties.phone.value AS company_phone,
      properties.state.value AS company_state,
      properties.lifecyclestage.value AS company_lifecycle_stage,
      properties.zip.value AS company_zip,
      properties.createdate.value AS company_created_date,
    from hubspot_companies

)

select * from companies_ds
