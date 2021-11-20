{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("ecommerce_warehouse_order_lines_sources") %}
{% if 'shopify_ecommerce' in var("ecommerce_warehouse_order_lines_sources") %}

with source AS (

  SELECT * FROM {{ ref('shopify__order_lines') }}


),
     order_lines_tax AS (

       SELECT
              order_line_id,
              title AS tax_type,
              price AS tax_amount,
              rate AS tax_rate
        FROM shopify.tax_line t
       where index =
       (SELECT max(index) FROM shopify.tax_line d where d.order_line_id = t.order_line_id)

     ),
joined AS (
  SELECT
    o.*,
    t.tax_type,
    t.tax_amount,
    t.tax_rate
  FROM
    source o
  LEFT JOIN
    order_lines_tax t
  ON o.order_line_id = t.order_line_id
)
,
renamed AS (
    SELECT
      fulfillable_quantity ,
      fulfillment_service ,
      fulfillment_status ,
      is_gift_card ,
      grams ,
      order_line_id ,
      index ,
      name ,
      order_id ,
      pre_tax_price,
      price,
      product_id ,
      property_charge_interval_frequency ,
      property_shipping_interval_frequency ,
      property_shipping_interval_unit_type ,
      property_subscription_id ,
      quantity ,
      is_requiring_shipping ,
      sku ,
      is_taxable ,
      tax_type,
      tax_amount,
      tax_rate,
      title ,
      total_discount,
      variant_id ,
      vendor ,
      source_relation ,
      refunded_quantity,
      refunded_subtotal,
      quantity_net_refunds,
      subtotal_net_refunds,
      variant_created_at ,
      variant_updated_at ,
      inventory_item_id ,
      image_id ,
      variant_title ,
      variant_price,
      variant_sku ,
      variant_position ,
      variant_inventory_policy ,
      variant_compare_at_price,
      variant_fulfillment_service ,
      variant_inventory_management ,
      variant_is_taxable ,
      variant_barcode ,
      variant_grams,
      variant_inventory_quantity ,
      variant_weight,
      variant_weight_unit ,
      variant_option_1 ,
      variant_option_2 ,
      variant_option_3 ,
      variant_tax_code ,
      variant_is_requiring_shipping
    FROM joined
)
SELECT * FROM renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
