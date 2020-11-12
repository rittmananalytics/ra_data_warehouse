{% if not var("enable_finance_warehouse") or not var("enable_xero_accounting_source") %}
  {{ config(
    enabled = false
  ) }}
{% endif %}

WITH payments_merge_list AS (

  SELECT
    *
  FROM
    {{ ref('stg_xero_accounting_payments') }}
)
SELECT
  *
FROM
  payments_merge_list
