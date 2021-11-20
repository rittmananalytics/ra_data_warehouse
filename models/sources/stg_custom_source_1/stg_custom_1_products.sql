{% if not var("enable_custom_source_1") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS (
    SELECT *
    from
    {{ source('custom_source_1','s_products' ) }}
),
renamed AS (
  SELECT
      CONCAT('custom_1-',id) AS product_id,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_name,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_description,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_manufacturer,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_brand,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_is_digital,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_is_active,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_subcategory_id,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_subcategory_name,
      CAST(null AS numeric) AS product_category_id,
      CAST(null AS numeric) AS product_category_name,
      CAST(null AS numeric) AS product_type_id,
      CAST(null AS numeric) AS product_type_name,
      CAST(null AS numeric) AS product_sku,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_color,
      CAST(null AS {{ dbt_utils.type_string() }})  AS product_size,
      CAST(null AS numeric)  AS product_pack_size,
      CAST(null AS numeric)  AS product_price,
      CAST(null AS numeric)  AS product_cost,
       CAST(null AS {{ dbt_utils.type_timestamp() }}) AS transaction_created_ts,
       CAST(null AS {{ dbt_utils.type_timestamp() }}) AS transaction_updated_ts
  FROM
    source
)
SELECT * FROM renamed
