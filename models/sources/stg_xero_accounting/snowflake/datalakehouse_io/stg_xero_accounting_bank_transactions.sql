{{config(enabled = target.type == 'snowflake')}}
{% if var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources") %}
{% if 'xero_accounting' in (var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources")) %}

{% if var("stg_xero_accounting_etl") == 'datalakehouse_io' %}

SELECT
  concat('{{ var('stg_xero_accounting_id-prefix') }}',t.bank_transaction_id) as bank_transaction_id,
  concat('{{ var('stg_xero_accounting_id-prefix') }}',t.bank_account_id) as bank_account_id,
  concat('{{ var('stg_xero_accounting_id-prefix') }}',t.contact_id) as contact_id,
  concat('{{ var('stg_xero_accounting_id-prefix') }}',t.currency_code) as currency_code,
  a.account_id,
  t.currency_rate,
  cast(t.trans_date as {{ dbt_utils.type_timestamp() }}) AS transaction_ts,  
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
   {{ source('datalakehouse_xero_accounting','bank_transaction') }} t
JOIN
  {{ source('datalakehouse_xero_accounting','bank_transaction_line_items') }} tl
ON
  t.bank_transaction_id = tl.bank_transaction_id
LEFT JOIN
  {{ ref('stg_xero_accounting_accounts') }} a
ON tl.account_code = a.account_code

{% endif %}

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
