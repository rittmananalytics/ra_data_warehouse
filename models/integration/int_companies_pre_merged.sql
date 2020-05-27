{% if not var("enable_crm_warehouse") and not var("enable_finance_warehouse") and not var("enable_marketing_warehouse") and not var("enable_projects_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
{{
    config(
        materialized="table"
    )
}}
with t_companies_pre_merged as (

      {% if var("enable_hubspot_crm_source") %}
      SELECT *
      FROM   {{ ref('stg_hubspot_crm_companies') }}
      {% endif %}

      {% if var("enable_hubspot_crm_source") and var("enable_harvest_projects_source")  %}
      UNION ALL
      {% endif %}

      {% if var("enable_harvest_projects_source") is true %}
      SELECT *
      FROM   {{ ref('stg_harvest_projects_companies') }}
      {% endif %}

      {% if (var("enable_hubspot_crm_source") or var("enable_harvest_projects_source")) and var("enable_xero_accounting_source") %}
      UNION ALL
      {% endif %}

      {% if var("enable_xero_accounting_source") is true %}
      SELECT *
      FROM   {{ ref('stg_xero_accounting_companies') }}
      {% endif %}

      {% if (var("enable_hubspot_crm_source") or var("enable_harvest_projects_source") or var("enable_xero_accounting_source")) and var("enable_stripe_payments_source")  %}
      UNION ALL
      {% endif %}

      {% if var("enable_stripe_payments_source") is true %}
      SELECT *
      FROM   {{ ref('stg_stripe_payments_companies') }}
      {% endif %}





      {% if (var("enable_hubspot_crm_source") or var("enable_harvest_projects_source") or var("enable_xero_accounting_source") or var("enable_stripe_payments_source") ) and var("enable_asana_projects_source") %}
      UNION ALL
      {% endif %}

      {% if var("enable_asana_projects_source") is true %}
      SELECT *
      FROM   {{ ref('stg_asana_projects_companies') }}
      {% endif %}

      {% if (var("enable_hubspot_crm_source") or var("enable_harvest_projects_source") or var("enable_xero_accounting_source") or var("enable_stripe_payments_source") or var("enable_asana_projects_source")) and var("enable_jira_projects_source") %}
      UNION ALL
      {% endif %}

      {% if var("enable_jira_projects_source") is true %}
      SELECT *
      FROM   {{ ref('stg_jira_projects_companies') }}
      {% endif %}
    ),
companies_merge_list as (
    select *
    from   {{ ref('companies_merge_list') }}
),
all_company_ids as (
       SELECT company_name, array_agg(distinct company_id ignore nulls) as all_company_ids
       FROM t_companies_pre_merged
       group by 1),
all_company_addresses as (
       SELECT company_name, array_agg(struct(company_address,
                                             company_address2,
                                             case when length(trim(company_city)) = 0 then null else company_city end as company_city,
                                             case when length(trim(company_state)) = 0 then null else company_state end as company_state,
                                             case when length(trim(company_country)) = 0 then null else company_country end as company_country,
                                             case when length(trim(company_zip)) = 0 then null else company_zip  end as company_zip) ignore nulls) as all_company_addresses
       FROM t_companies_pre_merged
       group by 1),
grouped as (
      SELECT
      company_name,
      max(company_phone) as company_phone,
      max(company_website) as company_website,
      max(company_industry) as company_industry,
      max(company_linkedin_company_page) as company_linkedin_company_page,
      max(company_linkedin_bio) as company_linkedin_bio,
      max(company_twitterhandle) as company_twitterhandle,
      max(company_description) as company_description,
      max(company_finance_status) as company_finance_status,
      min(company_created_date) as company_created_date,
      max(company_last_modified_date) as company_last_modified_date
    from t_companies_pre_merged
      group by 1
),
joined as (
      SELECT i.all_company_ids,
      g.*,
      a.all_company_addresses
      FROM grouped g
      JOIN all_company_ids i ON g.company_name = i.company_name
      LEFT OUTER JOIN all_company_addresses a ON g.company_name = a.company_name
)
select * from joined
