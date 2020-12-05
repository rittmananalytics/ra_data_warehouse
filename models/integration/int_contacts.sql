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
    SELECT *
    FROM   {{ ref('stg_hubspot_crm_contacts') }}
    {% endif %}
    {% if var("enable_hubspot_crm_source") and var("enable_harvest_projects_source")  %}
    UNION ALL
    {% endif %}
    {% if var("enable_harvest_projects_source")  %}
    SELECT *
    FROM   {{ ref('stg_harvest_projects_contacts') }}
    {% endif %}
    {% if (var("enable_hubspot_crm_source") or var("enable_harvest_projects_source")) and var("enable_xero_accounting_source") %}
    UNION ALL
    {% endif %}
    {% if var("enable_xero_accounting_source")  %}
    SELECT *
    FROM   {{ ref('stg_xero_accounting_contacts') }}
    {% endif %}
    {% if (var("enable_hubspot_crm_source") or var("enable_harvest_projects_source") or var("enable_xero_accounting_source")) and var("enable_mailchimp_email_source") %}
    UNION ALL
    {% endif %}
    {% if var("enable_mailchimp_email_source")  %}
    SELECT *
    FROM   {{ ref('stg_mailchimp_email_contacts') }}
    {% endif %}
    {% if var("enable_jira_projects_source")  %}
    UNION ALL
    SELECT *
    FROM   {{ ref('stg_jira_projects_contacts') }}
    {% endif %}
    {% if var("enable_asana_projects_source")  %}
    UNION ALL
    SELECT *
    FROM   {{ ref('stg_asana_projects_contacts') }}
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
         group by 1),
contacts as (
   select all_contact_ids,
          case when c.contact_name like '%@%' then initcap(concat(split(c.contact_name,'@')[safe_offset(0)],' ',
              case when split(split(c.contact_name,'@')[safe_offset(1)],'.')[safe_offset(1)] not in ('com','co','net','gov','nl','edu','org','dk','gr')
              then split(c.contact_name,'@')[safe_offset(1)] else '' end
              ))
              else c.contact_name end as contact_name,
          job_title,
          contact_phone,
          contact_mobile_phone,
          contact_is_contractor,
          contact_is_staff,
          contact_weekly_capacity,
          contact_default_hourly_rate,
          contact_cost_rate,
          contact_is_active,
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
                max(contact_last_modified_date) as contact_last_modified_date,
                max(user_is_contractor)         as contact_is_contractor,
                max(user_is_staff) as contact_is_staff,
                max(user_weekly_capacity)          as contact_weekly_capacity,
                max(user_default_hourly_rate)          as contact_default_hourly_rate,
                max(user_cost_rate)           as contact_cost_rate,
                max(user_is_active)                          as contact_is_active
            FROM t_contacts_merge_list
         group by 1) c
  join contact_emails e on c.contact_name = e.contact_name
  join contact_ids i on c.contact_name = i.contact_name
  join contact_company_addresses a on c.contact_name = a.contact_name
  join contact_company_ids cc on c.contact_name = cc.contact_name)
select * from contacts
