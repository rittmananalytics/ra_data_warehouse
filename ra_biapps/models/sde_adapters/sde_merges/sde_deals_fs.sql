with sde_deals_fs_merge_list as
  (
    SELECT * except (deal_id),
           deal_id as hubspot_deal_id
    FROM   {{ ref('sde_hubspot_crm_deals') }}
  ),
  companies_merge_list as (
      select *
      from   {{ ref('companies_merge_list') }}
  ),
  deals_companies_switched as (
    select coalesce(m.company_id,c.company_id) as company_id,
           c.* except(company_id),
    from sde_deals_fs_merge_list c
    left outer join companies_merge_list m
    on c.company_id = m.old_company_id
  )
select * from deals_companies_switched
