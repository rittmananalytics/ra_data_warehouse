{{config(enabled = target.type == 'redshift')}}
{% if var("ecommerce_warehouse_order_sources") %}
{% if 'segment_shopify_events' in var("ecommerce_warehouse_order_sources") %}

WITH orders AS (
	SELECT
		*
	FROM
		{{ var('stg_segment_shopify_events_segment_order_completed_table') }}
	WHERE
		is_valid_json_array (products)
),
refunded AS (
	SELECT
		order_id,
		timestamp AS order_refunded_ts,
		presentment_amount AS presentment_refunded_amount
	FROM
		{{ var('stg_segment_shopify_events_segment_order_refunded_table') }}
),
deleted AS (
	SELECT
		order_id,
		timestamp AS order_deleted_ts
	FROM
		{{ var('stg_segment_shopify_events_segment_order_deleted_table') }}
)
SELECT
	orders.order_id,
	checkout_id as order_checkout_id,
	event,
	event_text,
	original_timestamp AS order_ts,
	md5(concat(concat(concat(json_extract_path_text(replace(replace(products, '[', ''), ']', ''), 'shopify_product_id')::varchar, json_extract_path_text(replace(replace(products, '[', ''), ']', ''), 'shopify_variant_id')::varchar), json_extract_path_text(replace(replace(products, '[', ''), ']', ''), 'category')::varchar), json_extract_path_text(replace(replace(products, '[', ''), ']', ''), 'variant')::varchar)) AS product_uid,
	user_id AS user_id,
	presentment_amount,
	shipping,
	tax,
	currency,
	subtotal,
	total AS order_total,
	coupon,
	presentment_currency,
	discount,
	affiliation,
	order_refunded_ts,
	presentment_refunded_amount,
	CASE WHEN order_refunded_ts IS NULL
		AND order_deleted_ts IS NULL THEN
		TRUE
	ELSE
		FALSE
	END AS is_paid_order
FROM
	orders
	LEFT JOIN refunded ON orders.order_id = refunded.order_id
	LEFT JOIN deleted ON orders.order_id = deleted.order_id
ORDER BY
	1
{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
