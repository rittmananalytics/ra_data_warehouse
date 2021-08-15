{{config(enabled = target.type == 'redshift')}}
{% if var("ecommerce_warehouse_customer_cohorts_sources") %}
{% if 'shopify_ecommerce' in var("ecommerce_warehouse_customer_cohorts_sources") %}

with source as (

  select * from {{ ref('shopify__customer_cohorts') }}

),
renamed as (
    select
      date_month ,
      customer_id ,
      first_order_timestamp ,
      cohort_month ,
      source_relation ,
      order_count_in_month ,
      total_price_in_month ,
      line_item_count_in_month ,
      total_price_lifetime ,
      order_count_lifetime ,
      line_item_count_lifetime ,
      cohort_month_number ,
      customer_cohort_id
    from source
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
