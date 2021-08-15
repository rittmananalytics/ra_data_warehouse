{{config(enabled = target.type == 'redshift')}}
{% if var("ecommerce_warehouse_order_sources") %}
{% if 'shopify_ecommerce' in var("ecommerce_warehouse_order_sources") %}

with source as (

  select * from {{ ref('shopify__orders') }}


),
   order_tags as (

  select * from {{ var('stg_shopify_ecommerce_fivetran_order_tags_table') }}

),
renamed as (
    select
      billing_address_address_1 ,
      billing_address_address_2 ,
      billing_address_city ,
      billing_address_company ,
      billing_address_country ,
      billing_address_country_code ,
      billing_address_first_name ,
      billing_address_last_name ,
      billing_address_latitude ,
      billing_address_longitude ,
      billing_address_name ,
      billing_address_phone ,
      billing_address_province ,
      billing_address_province_code ,
      billing_address_zip ,
      browser_ip ,
      has_buyer_accepted_marketing ,
      cancel_reason ,
      cancelled_timestamp ,
      cart_token ,
      checkout_token ,
      closed_timestamp ,
      created_timestamp ,
      currency ,
      customer_id ,
      email ,
      financial_status ,
      fulfillment_status ,
      o.order_id ,
      landing_site_base_url ,
      location_id ,
      name ,
      note ,
      "number" ,
      order_number ,
      processed_timestamp ,
      processing_method ,
      referring_site ,
      total_shipping_price_set ,
      shipping_address_address_1 ,
      shipping_address_address_2 ,
      shipping_address_city ,
      shipping_address_company ,
      shipping_address_country ,
      shipping_address_country_code ,
      shipping_address_first_name ,
      shipping_address_last_name ,
      shipping_address_latitude ,
      shipping_address_longitude ,
      shipping_address_name ,
      shipping_address_phone ,
      shipping_address_province ,
      shipping_address_province_code ,
      shipping_address_zip ,
      source_name ,
      subtotal_price ,
      has_taxes_included ,
      is_test_order ,
      token ,
      total_discounts ,
      total_line_items_price ,
      total_price ,
      total_tax ,
      total_weight ,
      updated_timestamp ,
      user_id ,
      source_relation ,
      shipping_cost ,
      order_adjustment_amount ,
      order_adjustment_tax_amount ,
      refund_subtotal ,
      refund_total_tax ,
      order_adjusted_total ,
      line_item_count ,
      customer_order_seq_number ,
      new_vs_repeat,
      listagg(t.value,',') as order_tags
    from source o
    left join order_tags t
    on o.order_id = t.order_id
    {{ dbt_utils.group_by(n=76) }}
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
