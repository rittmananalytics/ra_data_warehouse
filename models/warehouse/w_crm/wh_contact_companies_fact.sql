{% if var("crm_warehouse_companies_sources") and var("crm_warehouse_contact_sources") %}


{{
    config(
        alias='contact_companies_fact'
    )
}}

with contacts AS (
  SELECT
    contact_pk, company_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest( all_contact_company_ids) AS company_id),
  companies AS (
  SELECT
    company_pk, company_id
  FROM
    {{ ref('wh_companies_dim') }},
    unnest( all_company_ids) AS company_id)
SELECT
      {{ dbt_utils.surrogate_key(
      ['contact_pk','company_pk']
      ) }} AS contact_company_pk,
       contact_pk,
       company_pk
FROM   contacts c
join   companies p
on     c.company_id = p.company_id

{% else %} {{config(enabled=false)}} {% endif %}
