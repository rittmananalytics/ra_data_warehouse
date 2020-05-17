{% if not var("enable_xero_accounting_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as
(
  {{ filter_stitch_table(var('stitch_accounts_table'),'accountid') }}

),
renamed as
(
select  accountid as            account_id,
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
