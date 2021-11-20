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
  companies_dim AS (
      SELECT *
      FROM {{ ref('wh_companies_dim') }}
  ),
  currencies_dim AS (
    SELECT *
    FROM {{ ref('wh_currencies_dim') }}
)
SELECT
   {{ dbt_utils.surrogate_key(['payment_id']) }} AS payment_pk,
   p.*
FROM
   payments p

{% else %} {{config(enabled=false)}} {% endif %}
