with customers as (

    select 
        {{ dbt_utils.star(from=ref('stg_shopify__customer'), except=["orders_count", "total_spent"]) }}
    from {{ var('shopify_customer') }}

), orders as (

    select *
    from {{ ref('shopify__customers__order_aggregates' )}}

), joined as (

    select 
        customers.*,
        orders.first_order_timestamp,
        orders.most_recent_order_timestamp,
        coalesce(orders.average_order_value, 0) as average_order_value,
        coalesce(orders.lifetime_total_spent, 0) as lifetime_total_spent,
        coalesce(orders.lifetime_total_refunded, 0) as lifetime_total_refunded,
        (coalesce(orders.lifetime_total_spent, 0) - coalesce(orders.lifetime_total_refunded, 0)) as lifetime_total_amount,
        coalesce(orders.lifetime_count_orders, 0) as lifetime_count_orders
    from customers
    left join orders
        using (customer_id, source_relation)

)

select *
from joined