{% if var("crm_warehouse_companies_sources") and var("crm_warehouse_contact_sources") %}


{{
    config(
        alias='contact_companies_fact'
    )
}}

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
      {{ dbt_utils.surrogate_key(
      ['contact_pk','company_pk']
      ) }} as contact_company_pk,
       contact_pk,
       company_pk
from   contacts c
join   companies p
on     c.company_id = p.company_id

{% else %} {{config(enabled=false)}} {% endif %}
