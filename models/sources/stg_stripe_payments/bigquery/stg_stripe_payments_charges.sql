{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_transaction_sources") %}
{% if 'stripe_payments' in var("finance_warehouse_transaction_sources") %}

WITH source AS (
    {{ filter_stitch_relation(relation=var('stg_stripe_payments_stitch_charges_table'),unique_column='id') }}
),

renamed as (

    select
    concat('{{ var('stg_stripe_payments_id-prefix') }}',id) as charge_id,
    concat('{{ var('stg_stripe_payments_id-prefix') }}',metadata.client_name) as customer_id,
    metadata.invoice_id as invoice_number,
    description as charge_description,
    created as payment_created_ts,
    created as charge_paid_at_ts,
    currency as charge_currency,
    amount as total_local_amount,
    paid as is_paid,
    'Sales' as charge_type
    from source
    where livemode is true
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
