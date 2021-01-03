{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_payment_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_payment_sources") %}

WITH
  source AS (
    {{ filter_stitch_relation(relation=var('stg_xero_accounting_stitch_payments_table'),unique_column='paymentid') }}

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

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
