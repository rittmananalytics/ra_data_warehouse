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
   {{ dbt_utils.surrogate_key(['currency_code']) }} as currency_pk,
   *
FROM
   currencies
