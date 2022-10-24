with order_line as (

    select *
    from {{ var('shopify_order_line') }}

), aggregated as (

    select 
        order_id,
        source_relation,
        count(*) as line_item_count
    from order_line
    group by 1,2

)

select *
from aggregated