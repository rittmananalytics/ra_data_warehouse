{% if var("finance_warehouse_payment_sources") %}

{{
    config(
        unique_key='payment_pk',
        alias='payments_fact'
    )
}}


WITH payments AS
  (
  SELECT *
  FROM   {{ ref('int_payments') }}
  ),
  companies_dim as (
      select *
      from {{ ref('wh_companies_dim') }}
  ),
  currencies_dim as (
    select *
    from {{ ref('wh_currencies_dim') }}
)
SELECT
   {{ dbt_utils.surrogate_key(['payment_id']) }} as payment_pk,
   p.*
FROM
   payments p

{% else %} {{config(enabled=false)}} {% endif %}
