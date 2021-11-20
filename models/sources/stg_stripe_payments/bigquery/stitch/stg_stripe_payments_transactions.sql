{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_transaction_sources") %}
{% if 'stripe_payments' in var("finance_warehouse_transaction_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=source('stitch_stripe_payments','transactions'),unique_column='id') }}
),
renamed AS (
  SELECT
      CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',id) AS transaction_id,
      description AS transaction_description,
      CAST(null AS {{ dbt_utils.type_string() }}) AS account_code,
      currency AS transaction_currency,
      exchange_rate AS transaction_exchange_rate,
      amount/100 AS transaction_gross_amount,
      fee/100 AS transaction_fee_amount,
      CAST(null AS numeric) AS transaction_tax_amount,
      net/100 AS transaction_net_amount,
      status AS transaction_status,
      type AS transaction_type,
      created AS transaction_created_ts,
      updated AS transaction_last_modified_ts,
  FROM
    source
)
SELECT
  *
FROM
  renamed

  {% else %} {{config(enabled=false)}} {% endif %}
  {% else %} {{config(enabled=false)}} {% endif %}
