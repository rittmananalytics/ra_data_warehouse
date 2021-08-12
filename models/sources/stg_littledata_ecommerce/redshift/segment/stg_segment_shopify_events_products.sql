{{config(enabled = target.type == 'redshift')}}
{% if var("crm_warehouse_contact_sources") %}
{% if 'segment_shopify_events' in var("crm_warehouse_contact_sources") %}

with products as (select
		products
      from
      	{{ var('stg_segment_shopify_events_segment_checkout_started_table') }}
      where IS_VALID_JSON_ARRAY (products)
      union all
      select
		products
      from
      	{{ var('stg_segment_shopify_events_segment_order_completed_table') }}
      where IS_VALID_JSON_ARRAY (products)

 ),
products_deduped as (
select   json_extract_path_text(replace(replace(products,'[',''),']',''), 'sku') as sku,
		 json_extract_path_text(replace(replace(products,'[',''),']',''), 'product_id') as product_id,
	   	 json_extract_path_text(replace(replace(products,'[',''),']',''), 'shopify_product_id') as shopify_product_id,
	     json_extract_path_text(replace(replace(products,'[',''),']',''), 'shopify_variant_id') as shopify_variant_id,
       	 json_extract_path_text(replace(replace(products,'[',''),']',''), 'brand') as brand,
	   	 json_extract_path_text(replace(replace(products,'[',''),']',''), 'category') as category,
	     json_extract_path_text(replace(replace(products,'[',''),']',''), 'name') as name,
	     json_extract_path_text(replace(replace(products,'[',''),']',''), 'price') as price,
	   	 json_extract_path_text(replace(replace(products,'[',''),']',''), 'variant') as variant,
	     case when REGEXP_COUNT ( products, 'Sample')>0 then true else false end as product_is_sample
from
   products
group by 1,2,3,4,5,6,7,8,9,10)
select md5(concat(concat(concat(shopify_product_id::varchar,shopify_variant_id::varchar),category::varchar),variant::varchar)) as product_uid,
       *
from products_deduped
order by 3,4,9

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
