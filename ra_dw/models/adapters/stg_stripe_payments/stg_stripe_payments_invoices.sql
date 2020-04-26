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
        {{ source('stitch_stripe','s_charges') }}
    )
  WHERE
    _sdc_batched_at = max_sdc_batched_at
),

renamed as (

    select

    metadata.invoice_id as invoice_number,
    concat('stripe-',metadata.client_name) as customer_id,
    concat('stripe-',id) as invoice_id,
    cast(null as string) as project_id,
    cast(null as string) as invoice_creator_users_id,
    description as invoice_subject,
    created as payment_created_ts,
    cast(null as timestamp) as invoice_issue_at_ts,
    cast(null as timestamp) as invoice_due_at_ts,
    cast(null as timestamp) as invoice_sent_at_ts,
    created as invoice_paid_at_ts,
    cast(null as timestamp) as invoice_period_start_at_ts,
    cast(null as timestamp) as invoice_period_end_at_ts,
    cast(null as numeric) as invoice_local_total_revenue_amount,
    currency as invoice_currency,
    amount as total_local_amount,
    cast(null as numeric) as invoice_local_total_billed_amount,
    cast(null as numeric) as invoice_local_total_services_amount,
    cast(null as numeric) as invoice_local_total_licence_referral_fee_amount,
    cast(null as numeric) as invoice_local_total_expenses_amount,
    cast(null as numeric) as invoice_local_total_support_amount,
    cast(null as string) as invoice_tax_rate_pct,
    cast(null as numeric) as invoice_local_total_tax_amount,
    cast(null as numeric) as invoice_local_total_due_amount,
    cast (null as string) as invoice_payment_term,
    case when paid then 'Paid'
         else 'Authorised' end as invoice_status,
    'Sales' as invoice_type
    from source
    where livemode is true
)
select * from renamed
