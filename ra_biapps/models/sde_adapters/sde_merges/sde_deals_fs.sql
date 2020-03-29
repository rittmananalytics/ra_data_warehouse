with sde_deals_fs_merge_list as
  (
    SELECT * except (deal_id),
           deal_id as hubspot_deal_id
    FROM   {{ ref('sde_hubspot_crm_deals') }}
  )
select * from sde_deals_fs_merge_list
