with orders as (

    select *
    from {{ var('shopify_order') }}

), transactions as (

    select *
    from {{ ref('shopify__transactions' )}}
    where lower(status) = 'success'
/*
), transaction_refund_adjustment as (

    select
        *,
        case when lower(kind) = 'refund'
            then currency_exchange_calculated_amount * -1
            else currency_exchange_calculated_amount
                end as adjusted_amount
    from transactions
*/

), aggregated as (

    select
        orders.customer_id,
        orders.source_relation,
        min(orders.created_timestamp) as first_order_timestamp,
        max(orders.created_timestamp) as most_recent_order_timestamp,
        avg(case when lower(transactions.kind) in ('sale','capture') then transactions.currency_exchange_calculated_amount end) as average_order_value,
        sum(case when lower(transactions.kind) in ('sale','capture') then transactions.currency_exchange_calculated_amount end) as lifetime_total_spent,
        sum(case when lower(transactions.kind) in ('refund') then transactions.currency_exchange_calculated_amount end) as lifetime_total_refunded,
        count(distinct orders.order_id) as lifetime_count_orders
    from orders
    left join transactions
        using (order_id, source_relation)
    where customer_id is not null
    group by 1,2

)

select *
from aggregated