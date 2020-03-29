{{
    config(
        unique_key='company_pk',
        alias='companies_dim'
    )
}}
WITH unique_companies AS
  (
  SELECT lower(company_name) as company_name
  FROM   {{ ref('sde_companies_ds') }}
  GROUP BY 1
  )
,
  unique_companies_with_uuid AS
  (
  SELECT company_name,
         GENERATE_UUID() as company_uid
  FROM   unique_companies
  )

SELECT
   GENERATE_UUID() as company_pk,
   c.company_id,
   u.company_uid,
   c.company_name,
   c.hubspot_company_id,
   c.xero_company_id,
   c.harvest_company_id,
   c.company_description,
   c.company_linkedin_company_page,
   c.company_twitterhandle,
   c.company_address,
   c.company_address2,
   c.company_city,
   c.company_country,
   c.company_website,
   c.company_contact_owner_id,
   c.company_industry,
   c.company_linkedin_bio,
   c.company_domain,
   c.company_phone,
   c.company_state,
   c.company_lifecycle_stage,
   c.company_zip,
   c.company_created_date
FROM
   {{ ref('sde_companies_ds') }} c
JOIN unique_companies_with_uuid  u
ON lower(c.company_name) = u.company_name
