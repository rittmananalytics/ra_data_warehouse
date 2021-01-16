{% set order_seq_number = "row_number() over (partition by customer_id order by created_at)" %}

with orders as (

  select * from {{ref('shopify_orders')}}

)

select
  *,
  {{ order_seq_number }} as order_seq_number,
  case
    when {{ order_seq_number }} = 1 then 'new'
    else 'repeat'
  end as new_vs_repeat,
  case
    when cancelled_at is not null then true
    else false
  end as cancelled
from orders
