with sde_communications_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_hubspot_crm_communications') }}
  )
select * from sde_communications_merge_list
