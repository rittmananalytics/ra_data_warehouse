{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources") %}
{% if 'xero_accounting' in (var("finance_warehouse_payment_sources") or var("finance_warehouse_invoice_sources")) %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_xero_accounting_stitch_currencies_table'),unique_column='code') }}

),
renamed as (
select concat('{{ var('stg_xero_accounting_id-prefix') }}',code) as currency_code,
       description as currency_name, from source)
select * from renamed

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
