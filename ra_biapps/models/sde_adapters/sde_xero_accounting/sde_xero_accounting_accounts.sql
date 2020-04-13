{% if not var("enable_xero_accounting_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with accounts as
(
  SELECT
    *
  FROM (
    SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY accountid ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      {{ source('xero_accounting', 'accounts') }})
  WHERE
    max_sdc_batched_at = _sdc_batched_at
  )
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
from accounts
