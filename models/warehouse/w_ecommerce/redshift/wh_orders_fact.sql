{% if var("ecommerce_warehouse_order_sources") %}

{{
    config(
        unique_key='order_pk',
        alias='orders_fact'
    )
}}


WITH orders AS
  (
  SELECT *
  FROM
     {{ ref('int_orders') }} o
),
  customers as
  (
    SELECT *
    FROM
       {{ ref('wh_customers_dim') }} o


  )
select    {{ dbt_utils.surrogate_key(
          ['order_id']
        ) }} as order_pk,
          c.customer_pk,
          o.*
FROM      orders o
LEFT JOIN customers c
ON        o.customer_id = c.customer_id


{% else %} {{config(enabled=false)}} {% endif %}
