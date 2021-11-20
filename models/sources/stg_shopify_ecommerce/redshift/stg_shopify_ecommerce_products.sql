{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("ecommerce_warehouse_product_sources") %}
{% if 'shopify_ecommerce' in var("ecommerce_warehouse_product_sources") %}

with source AS (

  SELECT * FROM {{ ref('shopify__products') }}


),
renamed AS (
    SELECT
      created_timestamp,
      handle ,
      product_id ,
      product_type ,
      published_timestamp,
      published_scope ,
      title ,
      updated_timestamp,
      vendor ,
      source_relation ,
      quantity_sold ,
      subtotal_sold ,
      quantity_sold_net_refunds ,
      subtotal_sold_net_refunds ,
      first_order_timestamp,
      most_recent_order_timestamp
    FROM source
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
