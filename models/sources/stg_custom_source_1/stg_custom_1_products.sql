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
      {{ cast() }}  as product_name,
      {{ cast() }}  as product_description,
      {{ cast() }}  as product_manufacturer,
      {{ cast() }}  as product_brand,
      {{ cast() }}  as product_is_digital,
      {{ cast() }}  as product_is_active,
      {{ cast() }}  as product_subcategory_id,
      {{ cast() }}  as product_subcategory_name,
      cast(null as numeric) as product_category_id,
      cast(null as numeric) as product_category_name,
      cast(null as numeric) as product_type_id,
      cast(null as numeric) as product_type_name,
      cast(null as numeric) as product_sku,
      {{ cast() }}  as product_color,
      {{ cast() }}  as product_size,
      cast(null as numeric)  as product_pack_size,
      cast(null as numeric)  as product_price,
      cast(null as numeric)  as product_cost,
       {{ cast(datatype='timestamp') }} as transaction_created_ts,
       {{ cast(datatype='timestamp') }} as transaction_updated_ts
  FROM
    source
)
select * from renamed
