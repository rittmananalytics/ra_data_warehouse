{% if var("crm_warehouse_deal_sources") and var("crm_warehouse_contact_sources") %}

{{
    config(
        alias='contact_deals_fact'
    )
}}

with contacts_dim AS (
  SELECT
    contact_pk,
    all_contact_ids AS contact_id
  FROM
    {{ ref('wh_contacts_dim') }},
    unnest (all_contact_ids) AS all_contact_ids
  ),
  deals_fact AS (
  SELECT
    *
  FROM
    {{ ref('wh_deals_fact') }}),
  contact_deals AS (
    SELECT
      *
    FROM
      {{ ref('int_contact_deals') }}
  )
SELECT
      {{ dbt_utils.surrogate_key(
      ['contact_pk','deal_pk']
      ) }} AS contact_deal_pk,
       contact_pk,
       deal_pk
FROM   contact_deals cd
join   contacts_dim c
on     cd.contact_id = c.contact_id
join   deals_fact d
on     cd.deal_id = d.deal_id

{% else %} {{config(enabled=false)}} {% endif %}
