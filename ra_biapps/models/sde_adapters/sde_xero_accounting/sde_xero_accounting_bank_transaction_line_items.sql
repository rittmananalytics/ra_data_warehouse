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
        {{ source('xero_accounting', 'bank_transactions') }})
    WHERE
      max_sdc_batched_at = _sdc_batched_at
    )
  SELECT
    banktransactionid AS bank_transaction_id,
    contact.contactid as bank_transaction_contact_id,
    bankaccount.name as bank_account_name,
    bankaccount.accountid as bank_account_id,
    currencycode as bank_transaction_currency_code,
    status as bank_transaction_status,
    reference as bank_transaction_reference,
    type as bank_transaction_type,
    date as bank_transaction_date,
    isreconciled as bank_transaction_is_reconciled,
    lineitems.lineitemid as bank_transaction_item_id,
    lineitems.accountcode as bank_transaction_line_item_account_code,
    lineitems.quantity as bank_transaction_line_item_quantity,
    lineitems.unitamount as bank_transaction_line_item_unit_amount,
    lineitems.taxtype as bank_transaction_line_item_tax_type,
    lineitems.description as bank_transaction_line_item_description,
    lineitems.lineamount as bank_transaction_line_item_line_amount,
    lineitems.taxamount as bank_transaction_line_item_tax_amount
  FROM
    bank_transactions i,
         UNNEST(i.lineitems) AS lineitems
