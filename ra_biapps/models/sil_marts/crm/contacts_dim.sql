{{
    config(
        materialized='table',
        unique_key='contact_pk'
    )
}}
WITH unique_contacts AS
  (
  SELECT lower(concat(coalesce(contact_name,''),':',coalesce(contact_email,''))) as contact_name_email
  FROM   {{ ref('sde_contacts_ds') }}
  GROUP BY 1
  )
,
  unique_contacts_with_uuid AS
  (
  SELECT contact_name_email,
         GENERATE_UUID() as contact_uid
  FROM   unique_contacts
  )

SELECT
   c.source,
   GENERATE_UUID() as contact_pk,
   u.contact_uid,
   c.contact_id,
   c.contact_first_name,
   c.contact_last_name,
   c.contact_name,
   c.contact_job_title,
   c.contact_email,
   c.contact_phone,
   c.contact_mobile_phone,
   c.contact_address,
   c.contact_city,
   c.contact_state,
   c.contact_postcode_zip,
   c.contact_company,
   c.contact_website,
   c.contact_company_id,
   c.contact_owner_id,
   c.contact_lifecycle_stage,
   c.contact_created_date,
   c.contact_last_modified_date
FROM
   {{ ref('sde_contacts_ds') }} c
JOIN unique_contacts_with_uuid  u
ON lower(concat(contact_name,':',coalesce(contact_email,''))) = contact_name_email
