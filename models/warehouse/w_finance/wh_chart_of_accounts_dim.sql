{% if not var("enable_finance_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='account_pk',
        alias='chart_of_accounts_dim'
    )
}}
{% endif %}

WITH chart_of_accounts AS
  (
  SELECT *
  FROM   {{ ref('int_chart_of_accounts') }}
  )

SELECT
   GENERATE_UUID() as account_pk,
   *
FROM
   chart_of_accounts
