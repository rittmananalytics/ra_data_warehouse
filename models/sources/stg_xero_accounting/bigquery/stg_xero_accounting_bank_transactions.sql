{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources") %}
{% if 'xero_accounting' in (var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources")) %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}

SELECT
  CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',t.bank_transaction_id) AS bank_transaction_id,
  CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',t.bank_account_id) AS bank_account_id,
  CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',t.contact_id) AS contact_id,
  CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',t.currency_code) AS currency_code,
  a.account_id,
  t.currency_rate,
  timestamp(t.date) AS transaction_ts,
  t.is_reconciled,
  t.line_amount_types,
  t.reference,
  t.status,
  t.sub_total,
  t.total,
  t.total_tax,
  tl.line_item_id,
  tl.account_code,
  tl.description,
  tl.item_code,
  tl.line_amount,
  tl.quantity,
  tl.tax_amount,
  tl.tax_type,
  tl.unit_amount,
  t.type,
  t.updated_date_utc
FROM
  {{ var('stg_xero_accounting_fivetran_bank_transactions_table') }} t
JOIN
  {{ var('stg_xero_accounting_fivetran_bank_transaction_line_items_table') }} tl
ON
  t.bank_transaction_id = tl.bank_transaction_id
LEFT JOIN
  {{ ref('stg_xero_accounting_accounts') }} a
ON tl.account_code = a.account_code

{% endif %}

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
