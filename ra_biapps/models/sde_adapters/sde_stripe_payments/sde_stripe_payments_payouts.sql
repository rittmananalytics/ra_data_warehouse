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
        {{ source('stitch_stripe','payouts') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
        id as payout_id,
        object as payout_object,
        balance_transaction as payout_balance_transaction,
        created as payout_created_ts,
        destination as payout_destination,
        currency as payout_currency,
        source_type as payout_source_type,
        arrival_date as payout_arrival_date,
        method as payout_method,
        bank_account as payout_bank_account,
        status as payout_status,
        livemode as payout_livemode,
        type as payout_type,
        amount as payout_amount,
        automatic as payout_automatic,
        date as payout_date,
        updated as payout_updated_ts,
        amount_reversed  as payout_amount_reversed,
        description  as payout_description

    from source

)

select * from renamed
