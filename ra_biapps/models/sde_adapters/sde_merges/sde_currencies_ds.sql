{% if not var("enable_finance_warehouse") or not var("enable_xero_accounting") %}
  {{ config(
    enabled = false
  ) }}
{% endif %}

WITH sde_currencies_merge_list AS (

  SELECT
    *
  FROM
    {{ ref('sde_xero_accounting_currencies') }}
)
SELECT
  *
FROM
  sde_currencies_merge_list
