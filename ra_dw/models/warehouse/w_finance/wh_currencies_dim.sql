{% if not var("enable_finance_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='currency_pk',
        alias='currency_dim'
    )
}}
{% endif %}

WITH currencies AS
  (
  SELECT *
  FROM   {{ ref('int_currencies') }}
  )

SELECT
   GENERATE_UUID() as currency_pk,
   *
FROM
   currencies
