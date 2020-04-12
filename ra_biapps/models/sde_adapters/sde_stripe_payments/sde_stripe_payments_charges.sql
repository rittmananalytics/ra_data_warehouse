{% if not enable_stripe_payments %}
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
        {{ source('stitch_stripe','charges') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select
    id as charge_id,
    object as charge_object,
    balance_transaction as charge_balance_transaction,
    created as charge_created_ts,
    payment_intent as charge_payment_intent,
    paid as charge_paid,
    currency as charge_currency,
    payment_method_details.type as charge_payment_method_type,
    payment_method_details.card.exp_year as charge_card_exp_year,
    payment_method_details.card.last4 as charge_card_last4,
    payment_method_details.card.country as charge_card_country,
    payment_method_details.card.brand as charge_card_brand,
    payment_method_details.card.checks as charge_card_checks,
    payment_method_details.card.checks.cvc_check as charge_card_cvc_check,
    payment_method_details.card.checks.address_postal_code_check as charge_card_address_postal_code,
    payment_method_details.card.fingerprint as charge_card_fingerprint,
    payment_method_details.card.network as charge_card_network,
    payment_method_details.card.exp_month as charge_card_exp_month,
    payment_method_details.card.funding as charge_card_funding,
    application as charge_application,
    status as charge_status,
    metadata.recipient_email as charge_recipient_email,
    metadata.invoice_id as charge_invoice_id,
    metadata.client_name as charge_client_name,
    captured as charge_captured,
    refunded as charge_refunded,
    livemode as charge_livemode,
    amount as charge_amount,
    amount_refunded as charge_amount_refunded,
    updated as charge_updated_ts,
    outcome.risk_level as charge_outcome_risk_level,
    outcome.seller_message as charge_outcome_seller_message,
    outcome.network_status as charge_outcome_network_status,
    outcome.type as charge_type,
    description as charge_description
    from source

)

select * from renamed
