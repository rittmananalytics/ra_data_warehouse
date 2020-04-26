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
        {{ source('stitch_stripe','s_payouts') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
    id as payout_id,
    amount/100 as payout_local_amount,
    amount_reversed/100 as payout_local_reversed_amount,
    status as payout_status,
    currency as payout_currency,
    arrival_date as payout_arrived_ts,
    bank_account.bank_name as payout_bank_name,
    bank_account.routing_number as payout_bank_sort_code,
    created as payout_created_ts
from source

)

select * from renamed
