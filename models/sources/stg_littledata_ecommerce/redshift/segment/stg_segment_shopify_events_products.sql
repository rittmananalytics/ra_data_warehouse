{{config(enabled = target.type == 'redshift')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'segment_shopify_events' in var("crm_warehouse_contact_sources") %}

with products AS (SELECT
		products
      from
      	{{ var('stg_segment_shopify_events_segment_checkout_started_table') }}
      where IS_VALID_JSON_ARRAY (products)
      union all
      SELECT
		products
      from
      	{{ var('stg_segment_shopify_events_segment_order_completed_table') }}
      where IS_VALID_JSON_ARRAY (products)

 ),
products_deduped AS (
SELECT   json_extract_path_text(replace(replace(products,'[',''),']',''), 'sku') AS sku,
		 json_extract_path_text(replace(replace(products,'[',''),']',''), 'product_id') AS product_id,
	   	 json_extract_path_text(replace(replace(products,'[',''),']',''), 'shopify_product_id') AS shopify_product_id,
	     json_extract_path_text(replace(replace(products,'[',''),']',''), 'shopify_variant_id') AS shopify_variant_id,
       	 json_extract_path_text(replace(replace(products,'[',''),']',''), 'brand') AS brand,
	   	 json_extract_path_text(replace(replace(products,'[',''),']',''), 'category') AS category,
	     json_extract_path_text(replace(replace(products,'[',''),']',''), 'name') AS name,
	     json_extract_path_text(replace(replace(products,'[',''),']',''), 'price') AS price,
	   	 json_extract_path_text(replace(replace(products,'[',''),']',''), 'variant') AS variant,
	     case when REGEXP_COUNT ( products, 'Sample')>0 then true else false end AS product_is_sample
from
   products
group by 1,2,3,4,5,6,7,8,9,10)
SELECT {{ dbt_utils.hash('CONCAT(
														CONCAT(
															CONCAT(
																CAST(shopify_product_id AS dbt_utils.type_string() ),
																CAST(shopify_variant_id AS dbt_utils.type_string() )
															),
															CAST(category AS dbt_utils.type_string() )
														),
														CAST(variant AS  dbt_utils.type_string() )
													)') }} AS product_uid,
       *
FROM products_deduped
order by 3,4,9

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
