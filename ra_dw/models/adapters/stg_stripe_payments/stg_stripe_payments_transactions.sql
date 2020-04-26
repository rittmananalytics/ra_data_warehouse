{% if not var("enable_stripe_payments_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
  SELECT
    * EXCEPT (_sdc_batched_at, max_sdc_batched_at)
  FROM
    (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY id ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('stitch_stripe','s_balance_transactions') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),
renamed AS (
  SELECT
      id as transaction_id,
      description as transaction_description,
      currency as transaction_currency,
      exchange_rate as transaction_exchange_rate,
      amount/100 as transaction_gross_amount,
      fee/100 as transaction_fee_amount,
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
