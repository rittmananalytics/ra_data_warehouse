{{
    config(
        unique_key='contact_pk',
        alias='contacts_dim'
    )
}}
WITH contacts AS
  (
  SELECT *
  FROM   {{ ref('sde_contacts_ds') }}
  )

SELECT
   GENERATE_UUID() as contact_pk,
   array_agg(c.contact_id) all contact_ids,
   contact_name,
   max(c.contact_job_title) as contact_job_title,
   array_agg(c.contact_email) all_emails contact_email,
   max(c.contact_phone) as contact_phone,
   max(c.contact_mobile_phone) as contact_mobile_phone,
   max(c.contact_address) as contact_address ,
   max(c.contact_city) as contact_city,
   max(c.contact_state) as contact_state,
   max(c.contact_postcode_zip) as contact_postcode_zip,
   max(c.contact_company) as contact_company,
   max(c.contact_website) as contact_website,
   array_agg(c.contact_company_id) all_contact_company_ids,
   max(c.contact_owner_id) as contact_owner_id,
   max(c.contact_lifecycle_stage) as contact_lifecycle_stage,
   max(c.contact_created_date) as contact_created_date,
   max(c.contact_last_modified_date) as contact_last_modified_date
FROM
   {{ ref('sde_contacts_ds') }} c
JOIN unique_contacts_with_uuid  u
ON lower(concat(contact_name,':',coalesce(contact_email,''))) = contact_name_email
group by 1,3
