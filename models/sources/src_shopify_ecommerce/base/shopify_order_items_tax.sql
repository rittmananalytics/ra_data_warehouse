select

	_sdc_level_0_id as item_id,
	_sdc_source_key_id as order_id,
	price,
	rate,
	title

from
  	{{var('order_items_tax_table')}}