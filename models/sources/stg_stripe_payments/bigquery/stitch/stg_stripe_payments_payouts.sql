{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_transaction_sources") %}
{% if 'stripe_payments' in var("finance_warehouse_transaction_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=var('stg_stripe_payments_stitch_payouts_table'),unique_column='id') }}
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

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
