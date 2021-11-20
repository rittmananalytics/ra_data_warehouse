{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_transaction_sources") %}
{% if 'stripe_payments' in var("finance_warehouse_transaction_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=source('stitch_stripe_payments','charges'),unique_column='id') }}
),

renamed AS (

    SELECT
    CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',id) AS charge_id,
    CONCAT('{{ var('stg_stripe_payments_id-prefix') }}',metadata.client_name) AS customer_id,
    metadata.invoice_id AS invoice_number,
    description AS charge_description,
    created AS payment_created_ts,
    created AS charge_paid_at_ts,
    currency AS charge_currency,
    amount AS total_local_amount,
    paid AS is_paid,
    'Sales' AS charge_type
    FROM source
    where livemode is true
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
