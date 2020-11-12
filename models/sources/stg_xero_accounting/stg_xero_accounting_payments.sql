{% if not var("enable_xero_accounting_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH
  source AS (
    {{ filter_stitch_table(var('stg_xero_accounting_stitch_schema'),var('stg_xero_accounting_stitch_payments_table'),'paymentid') }}
  ),
renamed as (
  SELECT
    concat('{{ var('stg_xero_accounting_id-prefix') }}',paymentid) as payment_id,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',account.accountid) as account_id,
    account.code as payment_code,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',invoice.contact.contactid) as company_id,
    invoice.isdiscounted as payment_is_discounted,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',invoice.currencycode) as currency_code,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',invoice.invoicenumber) as payment_invoice_number,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',invoice.invoiceid) as invoice_id,
    invoice.type as payment_invoice_type,
    status as payment_status,
    paymenttype as payment_type,
    reference as payment_reference,
    amount as payment_amount,
    date as payment_date,
    isreconciled as payment_is_reconciled,
    bankamount as payment_bank_amount,
    currencyrate as payment_currency_rate
  FROM source
)
select * from renamed
