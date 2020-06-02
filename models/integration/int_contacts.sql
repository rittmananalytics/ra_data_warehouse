{% if not var("enable_crm_warehouse") and not var("enable_finance_warehouse") and not var("enable_marketing_warehouse") and not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_contacts_merge_list as
  (
    {% if var("enable_hubspot_crm_source") %}
    SELECT * except (contact_id, contact_company_id),
           concat('hubspot-',contact_id) as contact_id,
           concat('hubspot-',contact_company_id) as contact_company_id
    FROM   {{ ref('stg_hubspot_crm_contacts') }}
    {% endif %}
    {% if var("enable_hubspot_crm_source") and var("enable_harvest_projects_source") is true %}
    UNION ALL
    {% endif %}
    {% if var("enable_harvest_projects_source") is true %}
    SELECT * except (contact_id, contact_company_id),
           concat('harvest-',contact_id) as contact_id,
           concat('harvest-',contact_company_id) as contact_company_id
    FROM   {{ ref('stg_harvest_projects_contacts') }}
    {% endif %}
    {% if (var("enable_hubspot_crm_source") or var("enable_harvest_projects_source")) and var("enable_xero_accounting_source") %}
    UNION ALL
    {% endif %}
    {% if var("enable_xero_accounting_source") is true %}
    SELECT * except (contact_id, contact_company_id),
           concat('xero-',contact_id) as contact_id,
           concat('xero-',contact_company_id) as contact_company_id
    FROM   {{ ref('stg_xero_accounting_contacts') }}
    {% endif %}
    {% if (var("enable_hubspot_crm_source") or var("enable_harvest_projects_source") or var("enable_xero_accounting_source")) and var("enable_mailchimp_email_source") %}
    UNION ALL
    {% endif %}
    {% if var("enable_mailchimp_email_source") is true %}
    SELECT * except (contact_id, contact_company_id),
           concat('mailchimp-',coalesce(contact_id,'')) as contact_id,
           concat('mailchimp-',coalesce(contact_company_id,'')) as contact_company_id
    FROM   {{ ref('stg_mailchimp_email_contacts') }}
    {% endif %}
  ),
   contact_emails as (
         SELECT contact_name, array_agg(distinct lower(contact_email) ignore nulls) as all_contact_emails
         FROM t_contacts_merge_list
         group by 1),
   contact_ids as (
         SELECT contact_name, array_agg(contact_id ignore nulls) as all_contact_ids
         FROM t_contacts_merge_list
         group by 1),
   contact_company_ids as (
               SELECT contact_name, array_agg(contact_company_id ignore nulls) as all_contact_company_ids
               FROM t_contacts_merge_list
               group by 1),
   contact_company_addresses as (
         select contact_name, ARRAY_AGG(STRUCT( contact_address, contact_city, contact_state, contact_country, contact_postcode_zip)) as all_contact_addresses
         FROM t_contacts_merge_list
         group by 1)
   contacts as (
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
  FROM t_contacts_merge_list
  group by 1) c
  join contact_emails e on c.contact_name = e.contact_name
  join contact_ids i on c.contact_name = i.contact_name
  join contact_company_addresses a on c.contact_name = a.contact_name
  join contact_company_ids cc on c.contact_name = cc.contact_name)

  enriched_contacts as
( WITH int_contacts as
  (SELECT contact_name,
          email
   FROM `ra-development.analytics_staging.int_contacts`,
   UNNEST (all_contact_emails) email),
enr_contacts as
  (SELECT * from `ra-development.analytics_staging.stg_clearbit_enrichment_contacts`
   WHERE contact_enrichment_full_name is not null),
joined as
  (SELECT i.*,
       e.*
   FROM int_contacts i
   LEFT OUTER JOIN enr_contacts e
   ON i.email = e.contact_enrichment_email),
filtered as (
   SELECT *
   EXCEPT (email, contact_enrichment_id, contact_enrichment_email),
   MAX(contact_enrichment_last_updated_at) over (partition by contact_name order by contact_enrichment_last_updated_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_contact_enrichment_last_updated_at
   FROM joined
   WHERE contact_enrichment_full_name is not null)
SELECT *
FROM filtered
WHERE contact_enrichment_last_updated_at = max_contact_enrichment_last_updated_at
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41)
