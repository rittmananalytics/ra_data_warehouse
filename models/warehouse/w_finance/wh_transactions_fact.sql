{% if not var("enable_finance_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='transaction_pk',
        alias='transactions_fact'
    )
}}
{% endif %}

WITH transactions AS
  (
  SELECT *
  FROM   {{ ref('int_transactions') }}
  )

SELECT
   GENERATE_UUID() as transaction_pk,
   *
FROM
   transactions
