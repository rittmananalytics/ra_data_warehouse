{% if not var("enable_finance_warehouse") or not var("enable_xero_accounting_source") %}
  {{ config(
    enabled = false
  ) }}
{% endif %}

WITH t_currencies_merge_list AS (

  SELECT
    *
  FROM
    {{ ref('stg_xero_accounting_currencies') }}
)
SELECT
  *
FROM
  t_currencies_merge_list
