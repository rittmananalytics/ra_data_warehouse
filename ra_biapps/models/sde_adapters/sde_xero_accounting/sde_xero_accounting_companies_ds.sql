WITH xero_companies as (

  SELECT * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
  (
    SELECT *,
           MAX(_sdc_batched_at) OVER (PARTITION BY contactid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM {{ source('xero_accounting', 'contacts') }}
  )
  WHERE _sdc_batched_at = max_sdc_batched_at
  AND lastname is not null

),

companies_ds as (

  WITH phones as (SELECT companies.contactid, phones.phonetype, phones.phonenumber, phones.phoneareacode, phones.phonecountrycode
    FROM xero_companies companies,
 unnest(phones) as phones
 ),
      addresses as (SELECT companies.contactid, addresses.addresstype, addresses.addressline1, addresses.addressline2, addresses.addressline3, addresses.addressline4, addresses.city, addresses.region, addresses.country, addresses.postalcode
         FROM xero_companies companies,
 unnest(addresses) as addresses
 )
 select
        'xero_accounting' as source,
        cast(contacts.contactid as string) as company_id,
        concat(contacts.firstname,' ',contacts.lastname) as company_name,
        string_agg(distinct addresses.addressline1) as company_address,
        string_agg(distinct addresses.city) as company_city,
        string_agg(distinct addresses.country) as company_country,
        replace(concat(replace(defaultphone.phonecountrycode,'+','00'),defaultphone.phoneareacode,defaultphone.phonenumber),' ','') as company_phone,
        string_agg(distinct addresses.region) as company_state,
        contacts.contactstatus as company_status,
        string_agg(distinct addresses.postalcode) as company_zip,
        cast(null as timestamp) as company_created_date,
        contacts.updateddateutc as company_last_modified_date
 from xero_companies contacts
 left outer join addresses as addresses
 on contacts.contactid = addresses.contactid
 and addresses.addresstype = 'STREET'
 left outer join phones as mobilephone
 on contacts.contactid = mobilephone.contactid
 and mobilephone.phonetype = 'MOBILE'
 left outer join phones as defaultphone
 on contacts.contactid = defaultphone.contactid
 and mobilephone.phonetype = 'DEFAULT'
 where replace(contacts.firstname,' ','') is not null
 group by 1,2,3,7,9,11,12)

select * from companies_ds
