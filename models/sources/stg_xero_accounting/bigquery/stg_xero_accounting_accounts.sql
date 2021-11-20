{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources") %}
{% if 'xero_accounting' in (var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources")) %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}

with source AS (
  SELECT *
  FROM {{ var('stg_xero_accounting_fivetran_accounts_table') }}

),
renamed as
(
SELECT  CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',account_id) AS            account_id,
        name AS                 account_name,
        code AS                 account_code,
        type AS                 account_type,
        class AS                account_class,
        status AS               account_status,
        description AS          account_description,
        reporting_code AS        account_reporting_code,
        reporting_code_name AS    account_reporting_code_name,
        currency_code AS         account_currency_code,
        bank_account_type AS      account_bank_account_type,
        bank_account_number AS    account_bank_account_number,
        system_account AS        account_is_system_account,
        tax_type AS              account_tax_type,
        show_in_expense_claims AS  account_show_in_expense_claims,
        enable_payments_to_account AS account_enable_payments_to_account
FROM source
)

SELECT * FROM renamed

{% endif %}

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
