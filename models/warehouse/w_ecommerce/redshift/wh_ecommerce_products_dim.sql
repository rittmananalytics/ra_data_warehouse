{% if var("ecommerce_warehouse_product_sources") %}

{{
    config(
        unique_key='product_pk',
        alias='products_dim'
    )
}}


WITH products AS
  (
  SELECT *
  FROM
     {{ ref('int_products') }} o
)
select    {{ dbt_utils.surrogate_key(
          ['product_id']
        ) }} as product_pk,
          *
FROM      products

{% else %} {{config(enabled=false)}} {% endif %}
