select

  id,
  barcode,
  created_at,
  fulfillment_service,
  grams,
  inventory_policy,
  inventory_quantity,
  position,
  price,
  product_id,
  requires_shipping,
  sku,
  taxable,
  title,
  weight,
  weight_unit
  
  
from
  {{ var('products_variants_table') }}