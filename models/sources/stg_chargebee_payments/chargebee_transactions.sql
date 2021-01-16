with source as (
  select
    *,
    max(_sdc_batched_at) over (partition by id order by _sdc_batched_at range between unbounded preceding and unbounded following) as max_sdc_batched_at
  from {{ source('chargebee', 'transactions') }}
),

renamed as (
  select
    subscription_id,
--    linked_invoices,
--    linked_invoices. value,
--    linked_invoices.value. invoice_total,
--    linked_invoices.value. applied_at,
--    linked_invoices.value. applied_amount,
--    linked_invoices.value. invoice_id,
--    linked_invoices.value. invoice_status,
    --linked_invoices.value. invoice_date,
    currency_code,
    payment_method,
    id as transaction_id,
    refunded_txn_id as refunded_transaction_id,
    l.value.cn_status as credit_note_status,
    l.value.applied_at as credit_note_applied_at,
    l.value.cn_reason_code as credit_note_reason_code,
    l.value.cn_date as credit_note_date,
    l.value.cn_total/100 as credit_note_total,
    l.value.applied_amount/100 as credit_note_applied_amount,
    l.value.cn_reference_invoice_id as credit_note_reference_invoice_id,
    l.value.cn_id as credit_note_id,
    updated_at as transaction_updated_at,
    error_text as transactions_error_text,
    status as transaction_status,
    base_currency_code,
    exchange_rate,
    customer_id as chargebee_customer_id,
    type as transaction_type,
    amount/100 as transaction_amount,
    payment_source_id,
    deleted as is_deleted,
    date as transaction_date
  from source s
  LEFT JOIN UNNEST(s.linked_credit_notes) l
  WHERE _sdc_batched_at = max_sdc_batched_at
)

select * from renamed
