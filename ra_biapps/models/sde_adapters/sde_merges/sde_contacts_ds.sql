with sde_contacts_ds_merge_list as
  (
    SELECT * except (contact_id, contact_company_id, source),
           concat('hubspot-',contact_id) as contact_id,
           concat('hubspot-',contact_company_id) as contact_company_id
    FROM   {{ ref('sde_hubspot_crm_contacts_ds') }}
    UNION ALL
    SELECT * except (contact_id, contact_company_id, source),
           concat('xero-',contact_id) as contact_id,
           concat('xero-',contact_company_id) as contact_company_id
    FROM   {{ ref('sde_xero_accounting_contacts_ds') }}
    UNION ALL
    SELECT * except (contact_id, contact_company_id, source),
           concat('harvest-',contact_id) as contact_id,
           concat('harvest-',contact_company_id) as contact_company_id
    FROM   {{ ref('sde_harvest_projects_contacts_ds') }}
    UNION ALL
    SELECT * except (contact_id, contact_company_id, source),
           concat('mailchimp-',coalesce(contact_id,'')) as contact_id,
           concat('mailchimp-',coalesce(contact_company_id,'')) as contact_company_id
    FROM   {{ ref('sde_mailchimp_email_contacts_ds') }}
  ),
  contact_emails as (
         SELECT contact_name, array_agg(distinct lower(contact_email) ignore nulls) as all_contact_emails
         FROM sde_contacts_ds_merge_list
         group by 1),
   contact_ids as (
         SELECT contact_name, array_agg(contact_id ignore nulls) as all_contact_ids
         FROM sde_contacts_ds_merge_list
         group by 1),
   contact_company_ids as (
               SELECT contact_name, array_agg(contact_company_id ignore nulls) as all_contact_company_ids
               FROM sde_contacts_ds_merge_list
               group by 1),
   contact_company_addresses as (
         select contact_name, ARRAY_AGG(STRUCT( contact_address, contact_city, contact_state, contact_country, contact_postcode_zip)) as all_contact_addresses
         FROM sde_contacts_ds_merge_list
         group by 1)
  select all_contact_ids,
          c.contact_name,
          job_title,
          contact_phone,
          contact_mobile_phone,
          contact_created_date,
          contact_last_modified_date,
          e.all_contact_emails,
          a.all_contact_addresses,
          cc.all_contact_company_ids
         from (
  select contact_name,
  max(contact_job_title) as job_title,
  max(contact_phone) as contact_phone,
  max(contact_mobile_phone) as contact_mobile_phone ,
  min(contact_created_date) as contact_created_date,
  max(contact_last_modified_date) as contact_last_modified_date
  FROM sde_contacts_ds_merge_list
  group by 1) c
  join contact_emails e on c.contact_name = e.contact_name
  join contact_ids i on c.contact_name = i.contact_name
  join contact_company_addresses a on c.contact_name = a.contact_name
  join contact_company_ids cc on c.contact_name = cc.contact_name
