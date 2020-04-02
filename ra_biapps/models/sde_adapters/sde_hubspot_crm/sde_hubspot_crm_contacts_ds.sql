WITH hubspot_contacts as (

  SELECT * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
  (
    SELECT *,
           MAX(_sdc_batched_at) OVER (PARTITION BY vid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM {{ source('hubspot_crm', 'contacts') }}
  )
  WHERE _sdc_batched_at = max_sdc_batched_at
  AND canonical_vid is not null

),

contacts_ds as (

    select

      'hubspot_crm' as source,
       cast(canonical_vid as string) as contact_id,
       properties.firstname.value as contact_first_name,
       properties.lastname.value as contact_last_name,
       coalesce(concat(properties.firstname.value,' ',properties.lastname.value),properties.email.value) as contact_name,
       properties.jobtitle.value contact_job_title,
       properties.email.value as contact_email,
       properties.phone.value as contact_phone,
       properties.mobilephone.value as contact_mobile_phone,
       properties.address.value contact_address,
       properties.city.value contact_city,
       properties.state.value contact_state,
       properties.country.value as contact_country,
       properties.zip.value contact_postcode_zip,
       properties.company.value contact_company,
       properties.website.value contact_website,
       cast(properties.associatedcompanyid.value as string) as contact_company_id,
       cast(properties.hubspot_owner_id.value as string) as contact_owner_id,
       properties.lifecyclestage.value as contact_lifecycle_stage,
       properties.createdate.value as contact_created_date,
       properties.lastmodifieddate.value as contact_last_modified_date,
    from hubspot_contacts

)

select * from contacts_ds
