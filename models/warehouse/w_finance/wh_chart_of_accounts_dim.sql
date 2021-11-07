{% if var("finance_warehouse_invoice_sources") %}
{{
    config(
        unique_key='account_pk',
        alias='chart_of_accounts_dim'
    )
}}


WITH chart_of_accounts AS
  (
  SELECT *
  FROM   {{ ref('int_chart_of_accounts') }}
  )

SELECT
   {{ dbt_utils.surrogate_key(['account_id']) }} as account_pk,
   *
FROM
   chart_of_accounts
{% else %} {{config(enabled=false)}} {% endif %}
