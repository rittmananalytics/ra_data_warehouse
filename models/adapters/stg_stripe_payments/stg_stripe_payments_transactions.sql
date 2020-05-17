{% if not var("enable_stripe_payments_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    {{ filter_stitch_table(var('stitch_balance_transactions_table'),'id') }}

),
renamed AS (
  SELECT
      concat('{{ var('id-prefix') }}',id) as transaction_id,
      description as transaction_description,
      currency as transaction_currency,
      exchange_rate as transaction_exchange_rate,
      amount/100 as transaction_gross_amount,
      fee/100 as transaction_fee_amount,
      cast (null as numeric) as transaction_tax_amount,
      net/100 as transaction_net_amount,
      status as transaction_status,
      type as transaction_type,
      created as transaction_created_ts,
      updated as transaction_updated_ts,
  FROM
    source
)
SELECT
  *
FROM
  renamed
