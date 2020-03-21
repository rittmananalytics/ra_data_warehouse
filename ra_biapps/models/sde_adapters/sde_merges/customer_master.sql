{{
    config(
        materialized='table'
    )
}}

with combined_raw_companies as (

    select c.*,
           x.*
    from {{ ref('companies')}} c
    full outer join {{ ref('xero_contacts_deduped')}} x
    on lower(c.hubspot_company_name) = lower(x.contact_name)

),

    customer_merges as (
      with customer_dedupes as (SELECT d.*,
             case when d.customer_id like '%xero-%' then replace(d.customer_id,'xero-','') end as xero_contact_id,
             case when d.merge_customer_id like '%xero-%' then replace(d.merge_customer_id,'xero-','') end as xero_merge_contact_id,
      FROM {{ ref('customer_dedupes')}} d)
      select
      coalesce(
              case when m2.contact_id not like '%hubspot-%' and m2.contact_id is not null then concat('xero-',m2.contact_id)
              else m2.contact_id end,
              d.merge_customer_id) as merge_customer_id,
             coalesce(
              case when m.contact_id not like '%hubspot-%' and m.contact_id is not null then concat('xero-',m.contact_id)
              else m.contact_id end,
              d.customer_id) as customer_id
      from customer_dedupes d
      left outer join {{ ref('xero_contacts')}} m
      on d.xero_contact_id = m.contact_id
      left outer join {{ ref('xero_contacts')}} m2
      on d.xero_merge_contact_id = m2.contact_id
    )
,
combined_companies as (

    select
      coalesce(
        case when hubspot_company_id is not null then concat('hubspot-',hubspot_company_id) end,
        concat('xero-',contact_id)) as customer_id,
      coalesce(hubspot_company_name, contact_name) as customer_name,
      hubspot_company_id,
      contact_id as xero_contact_id,
      hubspot_first_deal_created_date,
      hubspot_twitterhandle,
      hubspot_country,
      hubspot_total_money_raised,
      hubspot_city,
      hubspot_annual_revenue,
      hubspot_website,
      hubspot_owner_id,
      hubspot_industry,
      hubspot_linkedin_bio,
      hubspot_is_public,
      hubspot_domain as web_domain,
      hubspot_created_date,
      hubspot_state,
      hubspot_lifecycle_stage,
      hubspot_description,
      hubspot_domain,
      contact_is_supplier,
      contact_accounts_payable_tax_type,
      contact_account_receiveable_tax_type,
      contact_tax_number,
      contact_is_customer,
      contact_default_currency,
      contact_status,
      row_number() over (partition by lower(coalesce(hubspot_company_name, contact_name))) as c_r

    from combined_raw_companies

),
combined_companies_deduped as (
   select coalesce(m.customer_id,c.customer_id) as customer_id,
   c.* except (customer_id)
  from combined_companies c
  left outer join customer_merges m
  on c.customer_id = m.merge_customer_id
),

combined_companies_deduped_merged_fields as (
  select
  c.customer_id as customer_id,
  max(c.customer_name) as customer_name,
  max(coalesce(case when c.customer_id like '%hubspot-%' then safe_cast(replace(c.customer_id,'hubspot-','') as int64) end,c.hubspot_company_id)) as hubspot_company_id,
  max(coalesce(c.xero_contact_id,case when c.customer_id like '%xero-%' then replace(c.customer_id,'xero-','') end)) as xero_contact_id,
  max(coalesce(c.hubspot_first_deal_created_date,hc.hubspot_first_deal_created_date)) as hubspot_first_deal_created_date,
  max(coalesce(c.hubspot_twitterhandle,hc.hubspot_twitterhandle)) as hubspot_twitterhandle,
  max(coalesce(c.hubspot_country,hc.hubspot_country)) as hubspot_country,
  max(coalesce(c.hubspot_total_money_raised,hc.hubspot_total_money_raised)) as hubspot_total_money_raised,
  max(coalesce(c.hubspot_city,hc.hubspot_city)) as hubspot_city,
  max(coalesce(c.hubspot_annual_revenue,hc.hubspot_annual_revenue)) as hubspot_annual_revenue,
  max(coalesce(c.hubspot_website,hc.hubspot_website)) as hubspot_website,
  max(coalesce(c.hubspot_owner_id,hc.hubspot_owner_id)) as hubspot_owner_id,
  max(coalesce(c.hubspot_industry,hc.hubspot_industry)) as hubspot_industry,
  max(coalesce(c.hubspot_linkedin_bio,hc.hubspot_linkedin_bio)) as hubspot_linkedin_bio,
  max(coalesce(c.hubspot_is_public,hc.hubspot_is_public)) as hubspot_is_public,
  max(coalesce(c.web_domain,hc.hubspot_domain)) as web_domain,
  max(coalesce(c.hubspot_created_date,hc.hubspot_created_date)) as hubspot_created_date,
  max(coalesce(c.hubspot_state,hc.hubspot_state)) as hubspot_state,
  max(coalesce(c.hubspot_lifecycle_stage,hc.hubspot_lifecycle_stage)) as hubspot_lifecycle_stage,
  max(coalesce(c.hubspot_description,hc.hubspot_description)) as hubspot_description,
  max(coalesce(c.hubspot_domain,hc.hubspot_domain)) as hubspot_domain,
  max(contact_is_supplier) as xero_is_supplier,
  max(contact_accounts_payable_tax_type) as xero_accounts_payable_tax_type,
  max(contact_account_receiveable_tax_type) as xero_account_receiveable_tax_type,
  max(contact_tax_number) as xero_tax_number,
  max(contact_is_customer) as xero_is_customer,
  max(contact_default_currency) as xero_default_currency,
  max(contact_status) as xero_customer_status,
  max(c_r) as c_r
from combined_companies_deduped c
left outer join {{ ref('companies')}} hc
on c.hubspot_company_id = hc.hubspot_company_id
group by 1
)
select * from combined_companies_deduped_merged_fields


--where c_r = 1
