{% if (not var("enable_xero_accounting_source") and not var("enable_stripe_payments_source")) or not var("enable_finance_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with transactions_merge_list as
  (
    {% if var("enable_xero_accounting_source") %}
    SELECT *
    FROM   {{ ref('stg_xero_accounting_transactions') }}
    {% endif %}
    {% if var("enable_xero_accounting_source") and var("enable_stripe_payments_source") %}
    UNION ALL
    {% endif %}
    {% if var("enable_stripe_payments_source") %}
    SELECT *
    FROM   {{ ref('stg_stripe_payments_transactions') }}
    {% endif %}
  )
select * from transactions_merge_list
