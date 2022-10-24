{% macro get_order_columns() %}

{% set columns = [
    {"name": "id", "datatype": dbt_utils.type_numeric(), "alias": "order_id"},
    {"name": "processed_at", "datatype": dbt_utils.type_timestamp(), "alias": "processed_timestamp"},
    {"name": "updated_at", "datatype": dbt_utils.type_timestamp(), "alias": "updated_timestamp"},
    {"name": "user_id", "datatype": dbt_utils.type_numeric()},
    {"name": "total_discounts", "datatype": dbt_utils.type_float()},
    {"name": "total_line_items_price", "datatype": dbt_utils.type_float()},
    {"name": "total_price", "datatype": dbt_utils.type_float()},
    {"name": "total_tax", "datatype": dbt_utils.type_float()},
    {"name": "source_name", "datatype": dbt_utils.type_string()},
    {"name": "subtotal_price", "datatype": dbt_utils.type_float()},
    {"name": "taxes_included", "datatype": "boolean", "alias": "has_taxes_included"},
    {"name": "total_weight", "datatype": dbt_utils.type_numeric()},
    {"name": "landing_site_base_url", "datatype": dbt_utils.type_string()},
    {"name": "location_id", "datatype": dbt_utils.type_numeric()},
    {"name": "name", "datatype": dbt_utils.type_string()},
    {"name": "note", "datatype": dbt_utils.type_string()},
    {"name": "number", "datatype": dbt_utils.type_numeric()},
    {"name": "order_number", "datatype": dbt_utils.type_numeric()},
    {"name": "cancel_reason", "datatype": dbt_utils.type_string()},
    {"name": "cancelled_at", "datatype": dbt_utils.type_timestamp(), "alias": "cancelled_timestamp"},
    {"name": "cart_token", "datatype": dbt_utils.type_string()},
    {"name": "checkout_token", "datatype": dbt_utils.type_string()},
    {"name": "closed_at", "datatype": dbt_utils.type_timestamp(), "alias": "closed_timestamp"},
    {"name": "created_at", "datatype": dbt_utils.type_timestamp(), "alias": "created_timestamp"},
    {"name": "currency", "datatype": dbt_utils.type_string()},
    {"name": "customer_id", "datatype": dbt_utils.type_numeric()},
    {"name": "email", "datatype": dbt_utils.type_string()},
    {"name": "financial_status", "datatype": dbt_utils.type_string()},
    {"name": "fulfillment_status", "datatype": dbt_utils.type_string()},
    {"name": "processing_method", "datatype": dbt_utils.type_string()},
    {"name": "referring_site", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_address_1", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_address_2", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_city", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_company", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_country", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_country_code", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_first_name", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_last_name", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_latitude", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_longitude", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_name", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_phone", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_province", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_province_code", "datatype": dbt_utils.type_string()},
    {"name": "billing_address_zip", "datatype": dbt_utils.type_string()},
    {"name": "browser_ip", "datatype": dbt_utils.type_string()},
    {"name": "buyer_accepts_marketing", "datatype": "boolean", "alias": "has_buyer_accepted_marketing"},
    {"name": "total_shipping_price_set", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_address_1", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_address_2", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_city", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_company", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_country", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_country_code", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_first_name", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_last_name", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_latitude", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_longitude", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_name", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_phone", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_province", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_province_code", "datatype": dbt_utils.type_string()},
    {"name": "shipping_address_zip", "datatype": dbt_utils.type_string()},
    {"name": "test", "datatype": "boolean", "alias": "is_test_order"},
    {"name": "token", "datatype": dbt_utils.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_customer_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "accepts_marketing", "datatype": "boolean", "alias": "has_accepted_marketing"},
    {"name": "created_at", "datatype": dbt_utils.type_timestamp(), "alias": "created_timestamp"},
    {"name": "default_address_id", "datatype": dbt_utils.type_numeric()},
    {"name": "email", "datatype": dbt_utils.type_string()},
    {"name": "first_name", "datatype": dbt_utils.type_string()},
    {"name": "id", "datatype": dbt_utils.type_numeric(), "alias": "customer_id"},
    {"name": "last_name", "datatype": dbt_utils.type_string()},
    {"name": "orders_count", "datatype": dbt_utils.type_numeric()},
    {"name": "phone", "datatype": dbt_utils.type_string()},
    {"name": "state", "datatype": dbt_utils.type_string(), "alias": "account_state"},
    {"name": "tax_exempt", "datatype": "boolean", "alias": "is_tax_exempt"},
    {"name": "total_spent", "datatype": dbt_utils.type_float()},
    {"name": "updated_at", "datatype": dbt_utils.type_timestamp(), "alias": "updated_timestamp"},
    {"name": "verified_email", "datatype": "boolean", "alias": "is_verified_email"}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_order_line_refund_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "id", "datatype": dbt_utils.type_numeric(), "alias": "order_line_refund_id"},
    {"name": "location_id", "datatype": dbt_utils.type_numeric()},
    {"name": "order_line_id", "datatype": dbt_utils.type_numeric()},
    {"name": "subtotal", "datatype": dbt_utils.type_numeric()},
    {"name": "total_tax", "datatype": dbt_utils.type_numeric()},
    {"name": "quantity", "datatype": dbt_utils.type_float()},
    {"name": "refund_id", "datatype": dbt_utils.type_numeric()},
    {"name": "restock_type", "datatype": dbt_utils.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_order_line_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "fulfillable_quantity", "datatype": dbt_utils.type_numeric()},
    {"name": "fulfillment_service", "datatype": dbt_utils.type_string()},
    {"name": "fulfillment_status", "datatype": dbt_utils.type_string()},
    {"name": "gift_card", "datatype": "boolean", "alias": "is_gift_card"},
    {"name": "grams", "datatype": dbt_utils.type_numeric()},
    {"name": "id", "datatype": dbt_utils.type_numeric(), "alias": "order_line_id"},
    {"name": "index", "datatype": dbt_utils.type_numeric()},
    {"name": "name", "datatype": dbt_utils.type_string()},
    {"name": "order_id", "datatype": dbt_utils.type_numeric()},
    {"name": "pre_tax_price", "datatype": dbt_utils.type_float()},
    {"name": "price", "datatype": dbt_utils.type_float()},
    {"name": "product_id", "datatype": dbt_utils.type_numeric()},
    {"name": "property_charge_interval_frequency", "datatype": dbt_utils.type_numeric()},
    {"name": "property_for_shipping_jan_3_rd_2020", "datatype": dbt_utils.type_string()},
    {"name": "property_shipping_interval_frequency", "datatype": dbt_utils.type_numeric()},
    {"name": "property_shipping_interval_unit_type", "datatype": dbt_utils.type_string()},
    {"name": "property_subscription_id", "datatype": dbt_utils.type_numeric()},
    {"name": "quantity", "datatype": dbt_utils.type_numeric()},
    {"name": "requires_shipping", "datatype": "boolean", "alias": "is_requiring_shipping"},
    {"name": "sku", "datatype": dbt_utils.type_string()},
    {"name": "taxable", "datatype": "boolean", "alias": "is_taxable"},
    {"name": "title", "datatype": dbt_utils.type_string()},
    {"name": "total_discount", "datatype": dbt_utils.type_float()},
    {"name": "variant_id", "datatype": dbt_utils.type_numeric()},
    {"name": "vendor", "datatype": dbt_utils.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_product_columns() %}

{% set columns = [
    {"name": "_fivetran_deleted", "datatype": "boolean"},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "created_at", "datatype": dbt_utils.type_timestamp(), "alias": "created_timestamp"},
    {"name": "handle", "datatype": dbt_utils.type_string()},
    {"name": "id", "datatype": dbt_utils.type_numeric(), "alias": "product_id"},
    {"name": "product_type", "datatype": dbt_utils.type_string()},
    {"name": "published_at", "datatype": dbt_utils.type_timestamp(), "alias": "published_timestamp"},
    {"name": "published_scope", "datatype": dbt_utils.type_string()},
    {"name": "title", "datatype": dbt_utils.type_string()},
    {"name": "updated_at", "datatype": dbt_utils.type_timestamp(), "alias": "updated_timestamp"},
    {"name": "vendor", "datatype": dbt_utils.type_string()}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_product_variant_columns() %}

{% set columns = [
    {"name": "id", "datatype": dbt_utils.type_numeric(), "alias": "variant_id"},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "created_at", "datatype": dbt_utils.type_timestamp(), "alias": "created_timestamp"},
    {"name": "updated_at", "datatype": dbt_utils.type_timestamp(), "alias": "updated_timestamp"},
    {"name": "product_id", "datatype": dbt_utils.type_numeric()},
    {"name": "inventory_item_id", "datatype": dbt_utils.type_numeric()},
    {"name": "image_id", "datatype": dbt_utils.type_numeric()},
    {"name": "title", "datatype": dbt_utils.type_string()},
    {"name": "price", "datatype": dbt_utils.type_float()},
    {"name": "sku", "datatype": dbt_utils.type_string()},
    {"name": "position", "datatype": dbt_utils.type_numeric()},
    {"name": "inventory_policy", "datatype": dbt_utils.type_string()},
    {"name": "compare_at_price", "datatype": dbt_utils.type_float()},
    {"name": "fulfillment_service", "datatype": dbt_utils.type_string()},
    {"name": "inventory_management", "datatype": dbt_utils.type_string()},
    {"name": "taxable", "datatype": "boolean", "alias": "is_taxable"},
    {"name": "barcode", "datatype": dbt_utils.type_string()},
    {"name": "grams", "datatype": dbt_utils.type_float()},
    {"name": "inventory_quantity", "datatype": dbt_utils.type_numeric()},
    {"name": "weight", "datatype": dbt_utils.type_float()},
    {"name": "weight_unit", "datatype": dbt_utils.type_string()},
    {"name": "option_1", "datatype": dbt_utils.type_string()},
    {"name": "option_2", "datatype": dbt_utils.type_string()},
    {"name": "option_3", "datatype": dbt_utils.type_string()},
    {"name": "tax_code", "datatype": dbt_utils.type_string()},
    {"name": "old_inventory_quantity", "datatype": dbt_utils.type_numeric()},
    {"name": "requires_shipping", "datatype": "boolean", "alias": "is_requiring_shipping"}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_transaction_columns() %}

{% set columns = [
    {"name": "id", "datatype": dbt_utils.type_numeric(), "alias": "transaction_id"},
    {"name": "order_id", "datatype": dbt_utils.type_numeric()},
    {"name": "refund_id", "datatype": dbt_utils.type_numeric()},
    {"name": "amount", "datatype": dbt_utils.type_numeric()},
    {"name": "created_at", "datatype": dbt_utils.type_timestamp(), "alias": "created_timestamp"},
    {"name": "processed_at", "datatype": dbt_utils.type_timestamp(), "alias": "processed_timestamp"},
    {"name": "device_id", "datatype": dbt_utils.type_numeric()},
    {"name": "gateway", "datatype": dbt_utils.type_string()},
    {"name": "source_name", "datatype": dbt_utils.type_string()},
    {"name": "message", "datatype": dbt_utils.type_string()},
    {"name": "currency", "datatype": dbt_utils.type_string()},
    {"name": "location_id", "datatype": dbt_utils.type_numeric()},
    {"name": "parent_id", "datatype": dbt_utils.type_numeric()},
    {"name": "payment_avs_result_code", "datatype": dbt_utils.type_string()},
    {"name": "payment_credit_card_bin", "datatype": dbt_utils.type_string()},
    {"name": "payment_cvv_result_code", "datatype": dbt_utils.type_string()},
    {"name": "payment_credit_card_number", "datatype": dbt_utils.type_string()},
    {"name": "payment_credit_card_company", "datatype": dbt_utils.type_string()},
    {"name": "kind", "datatype": dbt_utils.type_string()},
    {"name": "receipt", "datatype": dbt_utils.type_string()},
    {"name": "currency_exchange_id", "datatype": dbt_utils.type_numeric()},
    {"name": "currency_exchange_adjustment", "datatype": dbt_utils.type_numeric()},
    {"name": "currency_exchange_original_amount", "datatype": dbt_utils.type_numeric()},
    {"name": "currency_exchange_final_amount", "datatype": dbt_utils.type_numeric()},
    {"name": "currency_exchange_currency", "datatype": dbt_utils.type_string()},
    {"name": "error_code", "datatype": dbt_utils.type_string()},
    {"name": "status", "datatype": dbt_utils.type_string()},
    {"name": "test", "datatype": "boolean"},
    {"name": "user_id", "datatype": dbt_utils.type_numeric()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()}
] %}

{% if target.type in ('redshift','postgres') %}
 {{ columns.append({"name": "authorization", "datatype": dbt_utils.type_string(), "quote": True, "alias": "authorization"}) }}
{% else %}
 {"name": "authorization", "datatype": dbt_utils.type_string()}
{% endif %}

{{ return(columns) }}

{% endmacro %}

{% macro get_refund_columns() %}

{% set columns = [
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()},
    {"name": "created_at", "datatype": dbt_utils.type_timestamp()},
    {"name": "id", "datatype": dbt_utils.type_numeric(), "alias": "refund_id"},
    {"name": "note", "datatype": dbt_utils.type_string()},
    {"name": "order_id", "datatype": dbt_utils.type_numeric()},
    {"name": "processed_at", "datatype": dbt_utils.type_timestamp()},
    {"name": "restock", "datatype": "boolean"},
    {"name": "user_id", "datatype": dbt_utils.type_numeric()}
] %}

{{ return(columns) }}

{% endmacro %}

{% macro get_order_adjustment_columns() %}

{% set columns = [
    {"name": "id", "datatype":  dbt_utils.type_numeric(), "alias": "order_adjustment_id"},
    {"name": "order_id", "datatype":  dbt_utils.type_numeric()},
    {"name": "refund_id", "datatype":  dbt_utils.type_numeric()},
    {"name": "amount", "datatype": dbt_utils.type_float()},
    {"name": "tax_amount", "datatype": dbt_utils.type_float()},
    {"name": "kind", "datatype": dbt_utils.type_string()},
    {"name": "reason", "datatype": dbt_utils.type_string()},
    {"name": "_fivetran_synced", "datatype": dbt_utils.type_timestamp()}
] %}

{{ return(columns) }}

{% endmacro %}
