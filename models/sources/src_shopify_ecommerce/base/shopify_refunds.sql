select

  id,
  order_id,
  user_id,
  restock,
  note,
  created_at

from
  {{ var('refunds_table') }}
