with
companies_pre_merged as (
select
      *
    from {{ ref('sde_companies_pre_merged') }}
),
companies_merge_list as (
    select *
    from   {{ ref('companies_merge_list') }}
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
  --join companies_pre_merged o
  --on m.company_id = o.company_id
  {{ dbt_utils.group_by(19) }}
),
company_merged_company_ids as (
  SELECT company_id, array_concat(array_agg(distinct company_id),array_agg(old_company_id)) all_company_ids
  FROM companies_merge_list group by 1
  union all
  select * from
  (select company_id, array_agg(distinct company_id) all_company_ids
  from companies_merged_ids
  where company_id not in (select company_id from companies_merge_list)
  group by 1
  )
),
companies_merged as (
  select c.company_id,
         c.company_name,
         l.all_company_ids,
         c.* except (company_id, company_name)
  from (
    select
         company_id,
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
   ) c
   left outer join company_merged_company_ids l
   on c.company_id = l.company_id )
,
 companies_not_merged as (
   select c.company_id,
         c.company_name,
         array_agg(l.company_id),
         c.* except (company_id, company_name)
  from (
   select
         company_id,
         company_name as company_name,
         company_description as company_description,
         company_linkedin_company_page as company_linkedin_company_page,
         company_twitterhandle as company_twitterhandle,
         company_address as company_address,
         company_address2 as company_address2,
         company_city as company_city,
         company_state as company_state,
         company_country as company_country,
         company_zip as company_zip,
         company_website as company_website,
         company_contact_owner_id as company_contact_owner_id,
         company_industry as company_industry,
         company_linkedin_bio as company_linkedin_bio,
         company_domain as company_domain,
         company_phone as company_phone,
         company_lifecycle_stage as company_lifecycle_stage,
         company_created_date as company_created_date
  from companies_pre_merged m
         where m.company_id not in (select company_id from companies_merge_list)
         and   m.company_id not in (select old_company_id from companies_merge_list)
   ) c
   left outer join companies_pre_merged l
   on c.company_id = l.company_id
   group by 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),
  companies_ds as (
  select * from companies_merged
  union all
  select * from companies_not_merged
  )
select * from companies_ds
