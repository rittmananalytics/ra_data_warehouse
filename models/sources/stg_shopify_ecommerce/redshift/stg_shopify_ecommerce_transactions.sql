{{config(enabled = target.type == 'redshift')}}
{% if var("ecommerce_warehouse_transaction_sources") %}
{% if 'shopify_ecommerce' in var("ecommerce_warehouse_transaction_sources") %}

with source as (

  select * from {{ ref('shopify__transactions') }}

),
renamed as (
    select
      transaction_id ,
      order_id ,
      refund_id ,
      amount ,
      created_timestamp ,
      processed_timestamp ,
      device_id ,
      gateway ,
      source_name ,
      message ,
      currency ,
      location_id ,
      parent_id ,
      payment_avs_result_code ,
      payment_credit_card_bin ,
      payment_cvv_result_code ,
      payment_credit_card_number ,
      payment_credit_card_company ,
      kind ,
      receipt ,
      currency_exchange_id ,
      currency_exchange_adjustment ,
      currency_exchange_original_amount ,
      currency_exchange_final_amount ,
      currency_exchange_currency ,
      error_code ,
      status ,
      test ,
      user_id ,
      "authorization" ,
      source_relation ,
      exchange_rate ,
      currency_exchange_calculated_amount
    from source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
