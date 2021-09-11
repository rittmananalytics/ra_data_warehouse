{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources") %}
{% if 'xero_accounting' in (var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources")) %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}

with source as (
  select *
  from {{ var('stg_xero_accounting_fivetran_accounts_table') }}

),
renamed as
(
select  concat('{{ var('stg_xero_accounting_id-prefix') }}',account_id) as            account_id,
        name as                 account_name,
        code as                 account_code,
        type as                 account_type,
        class as                account_class,
        status as               account_status,
        description as          account_description,
        reporting_code as        account_reporting_code,
        reporting_code_name as    account_reporting_code_name,
        currency_code as         account_currency_code,
        bank_account_type as      account_bank_account_type,
        bank_account_number as    account_bank_account_number,
        system_account as        account_is_system_account,
        tax_type as              account_tax_type,
        show_in_expense_claims as  account_show_in_expense_claims,
        enable_payments_to_account as account_enable_payments_to_account
from source
)

select * from renamed

{% endif %}

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
