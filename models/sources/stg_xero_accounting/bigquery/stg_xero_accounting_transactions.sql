{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_transaction_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_transaction_sources") %}

WITH
  source AS
  (
    {{ filter_stitch_relation(relation=var('stg_xero_accounting_stitch_bank_transactions_table'),unique_column='banktransactionid') }}

  ),
renamed as (
  SELECT
      concat('{{ var('stg_xero_accounting_id-prefix') }}',banktransactionid) as transaction_id,
      lineitems.description as transaction_description,
      split(lineitems.accountcode,' ')[SAFE_OFFSET(0)] as account_code,
      currencycode as transaction_currency,
      cast(null as numeric) as transaction_exchange_rate,
      lineitems.lineamount as transaction_gross_amount,
      cast(null as numeric) as transaction_fee_amount,
      lineitems.taxamount as transaction_tax_amount,
      lineitems.lineamount - lineitems.taxamount as transaction_net_amount,
      case when isreconciled then 'Reconciled' else 'Unreconciled' end as transaction_status,
      type as transaction_type,
      date as transaction_created_ts,
      cast(null as timestamp) as transaction_last_modified_ts
  FROM
    source i,
         UNNEST(i.lineitems) AS lineitems)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
