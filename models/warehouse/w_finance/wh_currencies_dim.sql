{% if var("finance_warehouse_invoice_sources") %}

{{
    config(
        unique_key='currency_pk',
        alias='currency_dim'
    )
}}


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
{% else %} {{config(enabled=false)}} {% endif %}
