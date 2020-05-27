{% if not var("enable_stripe_payments_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    {{ filter_stitch_table(var('stitch_charges_table'),'id') }}
),

renamed as (

    select
    concat('{{ var('id-prefix') }}',id) as charge_id,
    concat('{{ var('id-prefix') }}',metadata.client_name) as customer_id,
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
