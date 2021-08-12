{% if var("ecommerce_warehouse_transaction_sources") %}

{{
    config(
        unique_key='transaction_pk',
        alias='ecommerce_transactions_fact'
    )
}}


WITH transactions AS
  (
  SELECT *
  FROM
     {{ ref('int_ecommerce_transactions') }} o
)
select    {{ dbt_utils.surrogate_key(
          ['transaction_id']
        ) }} as transaction_pk,
          *
FROM      transactions

{% else %} {{config(enabled=false)}} {% endif %}
