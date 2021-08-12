{% if var("ecommerce_warehouse_order_lines_sources") %}

{{
    config(
        unique_key='order_line_pk',
        alias='order_lines_fact'
    )
}}


WITH order_lines AS
  (
  SELECT *
  FROM
     {{ ref('int_order_lines') }} o
)
  ,
    products as
    (
      SELECT *
      FROM
         {{ ref('wh_products_dim') }} p


    )
    ,
  orders as
  (
    SELECT *
    FROM
       {{ ref('wh_orders_fact') }} p


  )
select    {{ dbt_utils.surrogate_key(
          ['l.order_id','l.order_line_id']
        ) }} as order_line_pk,
          o.order_pk,
          p.product_pk,
          l.*
FROM      order_lines l
LEFT JOIN orders o
ON        l.order_id = o.order_id
LEFT JOIN products p
ON        l.product_id = p.product_id

{% else %} {{config(enabled=false)}} {% endif %}
