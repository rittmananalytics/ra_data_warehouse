with combined_raw_companies as (

    select c.company_id,
           c.company_name,
           c.company_description,
           c.company_linkedin_company_page,
           c.company_twitterhandle,
           c.company_address,
           c.company_address2,
           c.company_city,
           c.company_country,
           c.company_website,
           c.company_contact_owner_id,
           c.company_industry,
           c.company_linkedin_bio,
           c.company_domain,
           c.company_phone,
           c.company_state,
           c.company_lifecycle_stage,
           c.company_zip,
           c.company_created_date,
           x.company_id as xero_company_id,
           x.company_name as  xero_company_name,
           x.company_address as  xero_company_address,
           x.company_city as  xero_company_city,
           x.company_country as  xero_company_country,
           x.company_phone as  xero_company_phone,
           x.company_state as  xero_company_state,
           x.company_zip as  xero_company_zip,
           x.company_created_date as  xero_company_created_date,
           h.company_id as harvest_company_id,
           h.company_name as harvest_company_name,
           h.company_address as harvest_company_address,
           h.company_created_date as harvest_company_created_date,
           h.company_last_modified_date as harvest_company_last_modified_date
    from {{ ref('sde_hubspot_crm_companies')}} c
    full outer join {{ ref('sde_xero_accounting_companies_ds')}} x on lower(c.company_name) = lower(x.company_name)
    full outer join {{ ref('sde_harvest_projects_companies_ds')}} h on lower(c.company_name) = lower(h.company_name)
  ),
companies_merge_list as (
    select *
    from   {{ ref('companies_merge_list') }}
),
companies_pre_merged as (
select
      case when company_id is not null then concat('hubspot-',company_id)
           when company_id is null and xero_company_id is not null then concat('xero-',xero_company_id)
           when company_id is null and xero_company_id is null and harvest_company_id is not null then concat('harvest-',harvest_company_id)
           end as company_id,
      coalesce(company_name, xero_company_name, harvest_company_name) as company_name,
      company_id as hubspot_company_id,
      xero_company_id,
      harvest_company_id,
      company_description,
      company_linkedin_company_page,
      company_twitterhandle,
      coalesce(company_address,xero_company_address,harvest_company_address) as company_address,
      company_address2,
      coalesce(company_city,xero_company_city) as company_city,
      coalesce(company_country,xero_company_country) as company_country,
      company_website,
      company_contact_owner_id,
      company_industry,
      company_linkedin_bio,
      company_domain,
      coalesce(company_phone,xero_company_phone) as company_phone,
      coalesce(company_state,xero_company_state) as company_state,
      company_lifecycle_stage,
      coalesce(company_zip,xero_company_zip) as company_zip,
      coalesce(company_created_date,xero_company_created_date,harvest_company_created_date) as company_created_date
    from combined_raw_companies
),
companies_merged_ids as (
  select
    coalesce(m.company_id,c.company_id) as company_id,
    coalesce(o.company_name,c.company_name) as company_name,
               coalesce(c.company_description,o.company_description) as company_description,
               coalesce(c.company_linkedin_company_page,o.company_linkedin_company_page) as company_linkedin_company_page,
               coalesce(c.company_twitterhandle,o.company_twitterhandle) as company_twitterhandle,
               coalesce(c.company_address,o.company_address) as company_address,
               coalesce(c.company_address2,o.company_address2) as company_address2,
               coalesce(c.company_city,o.company_city) as company_city,
               coalesce(c.company_country,o.company_country) as company_country,
               coalesce(c.company_website,o.company_website) as company_website,
               coalesce(c.company_contact_owner_id,o.company_contact_owner_id) as company_contact_owner_id,
               coalesce(c.company_industry,o.company_industry) as company_industry,
               coalesce(c.company_linkedin_bio,o.company_linkedin_bio) as company_linkedin_bio,
               coalesce(c.company_domain,o.company_domain) as company_domain,
               coalesce(c.company_phone,o.company_phone) as company_phone,
               coalesce(c.company_state,o.company_state) as company_state,
               coalesce(c.company_lifecycle_stage,o.company_lifecycle_stage) as company_lifecycle_stage,
               coalesce(c.company_zip,o.company_zip) as company_zip,
               coalesce(c.company_created_date,o.company_created_date) as company_created_date
    from companies_pre_merged c
  left outer join companies_merge_list m
  on c.company_id = m.old_company_id
  join companies_pre_merged o
  on m.company_id = o.company_id
  {{ dbt_utils.group_by(19) }}
),
companies_ds as (
  select company_id,
         max(company_name) as company_name,
         max(company_description) as company_description,
         max(company_linkedin_company_page) as company_linkedin_company_page,
         max(company_twitterhandle) as company_twitterhandle,
         max(company_address) as company_address,
         max(company_address2) as company_address2,
         max(company_city) as company_city,
         max(company_state) as company_state,
         max(company_country) as company_country,
         max(company_zip) as company_zip,
         max(company_website) as company_website,
         max(company_contact_owner_id) as company_contact_owner_id,
         max(company_industry) as company_industry,
         max(company_linkedin_bio) as company_linkedin_bio,
         max(company_domain) as company_domain,
         max(company_phone) as company_phone,
         max(company_lifecycle_stage) as company_lifecycle_stage,
         max(company_created_date) as company_created_date
  from companies_merged_ids m
         group by 1
  union deal
   select * except (hubspot_company_id, xero_company_id, harvest_company_id)
   from companies_pre_merged
   where company_id not in (select company_id from companies_merge_list)
  --where merge_company_id not in (select old_company_id
  --                         from companies_merge_list)
)

select * from companies_ds
