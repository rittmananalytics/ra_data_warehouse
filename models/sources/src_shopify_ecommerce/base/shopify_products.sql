select

  id,
  title,
  product_type,
  vendor,
  tags,
  handle,
  published_at,
  created_at,
  updated_at

from
  {{ var('products_table') }}
