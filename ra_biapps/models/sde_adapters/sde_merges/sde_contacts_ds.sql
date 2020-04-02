with sde_contacts_ds_merge_list as
  (
    SELECT * except (contact_id),
           concat('hubspot-',contact_id) as contact_id,
           concat('hubspot-',contact_company_id) as contact_company_id
    FROM   {{ ref('sde_hubspot_crm_contacts_ds') }}
    UNION ALL
    SELECT  except (contact_id),
           concat('xero-',contact_id) as contact_id,
           concat('xero-',contact_company_id) as contact_company_id
    FROM   {{ ref('sde_xero_accounting_contacts_ds') }}
    UNION ALL
    SELECT  except (contact_id),
           concat('harvest-',contact_id) as contact_id,
           concat('harvest-',contact_company_id) as contact_company_id
    FROM   {{ ref('sde_harvest_projects_contacts_ds') }}
    UNION ALL
    SELECT * except (contact_id),
           concat('mailchimp-',contact_id) as contact_id,
           concat('mailchimp-',contact_company_id) as contact_company_id
    FROM   {{ ref('sde_mailchimp_email_contacts_ds') }}
  )
select * from sde_contacts_ds_merge_list
