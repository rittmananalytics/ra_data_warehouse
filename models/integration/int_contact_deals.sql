{% if not var("enable_crm_warehouse") or not var("enable_hubspot_crm_source") %}
  {{ config(
    enabled = false
  ) }}
{% endif %}

WITH t_contact_deals_list AS (

  SELECT
    *
  FROM
    {{ ref('stg_hubspot_crm_contact_deals') }}
)
SELECT
  *
FROM
  t_contact_deals_list
