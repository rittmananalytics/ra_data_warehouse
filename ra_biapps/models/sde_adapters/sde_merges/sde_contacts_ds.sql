with sde_contacts_ds_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_hubspot_crm_contacts_ds') }}
    UNION ALL
    SELECT *
    FROM   {{ ref('sde_xero_accounting_contacts_ds') }}
    UNION ALL
    SELECT *
    FROM   {{ ref('sde_harvest_projects_contacts_ds') }}
  )
select * from sde_contacts_ds_merge_list
