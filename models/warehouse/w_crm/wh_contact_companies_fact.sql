{% if not var("enable_crm_warehouse") and not enable_finance_warehouse and not enable_marketing_warehouse and not enable_projects_warehouse %}

{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='contact_companies_fact'
    )
}}
{% endif %}
with contacts as (
  SELECT
    contact_pk, company_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest( all_contact_company_ids) as company_id),
  companies as (
  SELECT
    company_pk, company_id
  FROM
    {{ ref('wh_companies_dim') }},
    unnest( all_company_ids) as company_id)
select
       GENERATE_UUID() as contact_company_pk,
       contact_pk,
       company_pk
from   contacts c
join   companies p
on     c.company_id = p.company_id
