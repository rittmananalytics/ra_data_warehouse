WITH mailchimp_contacts as (

  SELECT * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
  (
    SELECT *,
           MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM {{ source('mailchimp_email', 'list_members') }}
  )
  WHERE _sdc_batched_at = max_sdc_batched_at

),

contacts_ds as (

  SELECT
      'mailchimp_email' as source,
      id as contact_id,
      merge_fields.fname as contact_first_name,
      merge_fields.lname as contact_last_name,
      concat(merge_fields.fname,' ',merge_fields.lname) as contact_name,
      cast(null as string) as contact_job_title,
      email_address as contact_email,
      merge_fields.phone as contact_phone,
      cast(null as string) as contact_mobile_phone,
      merge_fields.address__re.addr1 as contact_address,
      merge_fields.address__re.city as contact_city,
      merge_fields.address__re.state as contact_state,
      merge_fields.address__re.country as contact_country,
      merge_fields.address__re.zip as contact_postcode_zip,
      cast(null as string)  as contact_company,
      cast(null as string)  as contact_website,
      cast(null as string) as contact_company_id,
      cast(null as string) as contact_owner_id,
      status as contact_lifecycle_stage,
      timestamp_opt as contact_created_date,
      last_changed as contact_last_modified_date
FROM
  mailchimp_contacts
)

select * from contacts_ds
