{% if target.type == 'bigquery' or target.type == 'snowflake' or target.type == 'redshift' %}
{% if var("ecommerce_warehouse_customer_sources") %}
{% if 'shopify_ecommerce' in var("ecommerce_warehouse_customer_sources") %}

with source as (

  select * from {{ ref('shopify__customers') }}


),
   customer_tags as (

  select * from {{ source('fivetran_shopify', 'customer_tag') }}

),
renamed as (
    select
      created_timestamp ,
      default_address_id ,
      email ,
      c.customer_id ,
      account_state ,
      is_tax_exempt ,
      updated_timestamp ,
      is_verified_email ,
      source_relation ,
      first_order_timestamp ,
      most_recent_order_timestamp ,
      average_order_value ,
      lifetime_total_spent ,
      lifetime_total_refunded ,
      lifetime_total_amount ,
      lifetime_count_orders ,
      {{ fivetran_utils.string_agg(
        't.value',
        ','
      ) }} AS  as customer_tags

    from source c
    left join customer_tags t
    on c.customer_id = t.customer_id
    {{ dbt_utils.group_by(n=16) }}
)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
