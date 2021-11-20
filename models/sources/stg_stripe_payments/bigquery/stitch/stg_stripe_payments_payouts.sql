{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_transaction_sources") %}
{% if 'stripe_payments' in var("finance_warehouse_transaction_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=source('stitch_stripe_payments','payouts'),unique_column='id') }}
),

renamed AS (

    SELECT
    id AS payout_id,
    amount/100 AS payout_local_amount,
    amount_reversed/100 AS payout_local_reversed_amount,
    status AS payout_status,
    currency AS payout_currency,
    arrival_date AS payout_arrived_ts,
    bank_account.bank_name AS payout_bank_name,
    bank_account.routing_number AS payout_bank_sort_code,
    created AS payout_created_ts
FROM source

)

SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
