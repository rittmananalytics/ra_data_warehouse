{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_transaction_sources") %}
{% if 'stripe_payments' in var("finance_warehouse_transaction_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=var('stg_stripe_payments_stitch_balance_transactions_table'),unique_column='id') }}
),
renamed AS (
  SELECT
      concat('{{ var('stg_stripe_payments_id-prefix') }}',id) as transaction_id,
      description as transaction_description,
      cast(null as string) as account_code,
      currency as transaction_currency,
      exchange_rate as transaction_exchange_rate,
      amount/100 as transaction_gross_amount,
      fee/100 as transaction_fee_amount,
      cast (null as numeric) as transaction_tax_amount,
      net/100 as transaction_net_amount,
      status as transaction_status,
      type as transaction_type,
      created as transaction_created_ts,
      updated as transaction_last_modified_ts,
  FROM
    source
)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
