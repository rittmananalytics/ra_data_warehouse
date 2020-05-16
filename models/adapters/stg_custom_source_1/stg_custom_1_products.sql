{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    select *
    from
    {{ source('custom_source_1','s_products' ) }}
),
renamed as (
  SELECT
      concat('custom_1-',id) as product_id,
      cast(null as string)  as product_name,
      cast(null as string)  as product_description,
      cast(null as string)  as product_manufacturer,
      cast(null as string)  as product_brand,
      cast(null as string)  as product_is_digital,
      cast(null as string)  as product_is_active,
      cast(null as string)  as product_subcategory_id,
      cast(null as string)  as product_subcategory_name,
      cast(null as numeric) as product_category_id,
      cast(null as numeric) as product_category_name,
      cast(null as numeric) as product_type_id,
      cast(null as numeric) as product_type_name,
      cast(null as numeric) as product_sku,
      cast(null as string)  as product_color,
      cast(null as string)  as product_size,
      cast(null as numeric)  as product_pack_size,
      cast(null as numeric)  as product_price,
      cast(null as numeric)  as product_cost,
      cast(null as timestamp) as transaction_created_ts,
      cast(null as timestamp) as transaction_updated_ts
  FROM
    source
)
select * from renamed
