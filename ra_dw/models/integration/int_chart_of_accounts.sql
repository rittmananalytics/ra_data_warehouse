{% if not var("enable_finance_warehouse") or not var("enable_xero_accounting_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_chart_of_accounts_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_xero_accounting_accounts') }}
  )
SELECT * from t_chart_of_accounts_merge_list
