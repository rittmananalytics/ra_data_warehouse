{% if not var("enable_xero_accounting_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH
  bank_transactions AS
  (
    SELECT
      *
    FROM (
      SELECT
        *,
        MAX(_sdc_batched_at) OVER (PARTITION BY banktransactionid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
      FROM
        {{ source('xero_accounting', 's_bank_transactions') }})
    WHERE
      max_sdc_batched_at = _sdc_batched_at
    )
  SELECT
      banktransactionid as transaction_id,
      lineitems.description as transaction_description,
      currencycode as transaction_currency,
      cast(null as numeric) as transaction_exchange_rate,
      lineitems.lineamount as transaction_gross_amount,
      cast(null as numeric) as transaction_fee_amount,
      lineitems.taxamount as transaction_tax_amount,
      lineitems.lineamount - lineitems.taxamount as transaction_net_amount,
      case when isreconciled then 'Reconciled' else 'Unreconciled' end as transaction_status,
      type as transaction_type,
      date as transaction_created_ts,
      cast(null as timestamp) as transaction_updated_ts
  FROM
    bank_transactions i,
         UNNEST(i.lineitems) AS lineitems
