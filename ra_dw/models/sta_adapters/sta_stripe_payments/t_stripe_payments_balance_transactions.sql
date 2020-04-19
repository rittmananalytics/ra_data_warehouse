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
    id as balance_transaction_id,
    available_on as balance_transaction_available_ts,
    OBJECT as balance_transaction_object,
    created as balance_transaction_created_ts,
    currency as balance_transaction_currency,
    net as balance_transaction_net_amount,
    status as balance_transaction_status,
    fee as balance_transaction_fee_amount,
    source as balance_transaction_source,
    TYPE as balance_transaction_type,
    amount as balance_transaction_gross_amount,
    updated as balance_transaction_updated_ts,
    fee_details as balance_transaction_fee_details,
    description as balance_transaction_description,
    exchange_rate as balance_transaction_exchange_rate
  FROM
    source
)
SELECT
  *
FROM
  renamed
