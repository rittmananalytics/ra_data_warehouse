{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources") %}
{% if 'xero_accounting' in (var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources")) %}

with source as
(
  {{ filter_stitch_relation(relation=var('stg_xero_accounting_stitch_accounts_table'),unique_column='accountid') }}

),
renamed as
(
select  concat('{{ var('stg_xero_accounting_id-prefix') }}',accountid) as            account_id,
        name as                 account_name,
        code as                 account_code,
        type as                 account_type,
        class as                account_class,
        status as               account_status,
        description as          account_description,
        reportingcode as        account_reporting_code,
        reportingcodename as    account_reporting_code_name,
        currencycode as         account_currency_code,
        bankaccounttype as      account_bank_account_type,
        bankaccountnumber as    account_bank_account_number,
        systemaccount as        account_is_system_account,
        taxtype as              account_tax_type,
        showinexpenseclaims as  account_show_in_expense_claims,
        enablepaymentstoaccount as account_enable_payments_to_account
from source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
