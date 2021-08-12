{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_currencies_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_currencies_sources") %}

{% if var("stg_xero_accounting_etl") == 'stitch' %}

with source as (
  {{ filter_stitch_relation(relation=var('stg_xero_accounting_stitch_currencies_table'),unique_column='code') }}

),
renamed as (
select concat('{{ var('stg_xero_accounting_id-prefix') }}',code) as currency_code,
       description as currency_name, from source)

{% elif var("stg_xero_accounting_etl") == 'fivetran' %}

with source as (
  select *
  from {{ var('stg_xero_accounting_fivetran_currency_table') }}

),
renamed as (
select concat('{{ var('stg_xero_accounting_id-prefix') }}',code) as currency_code,
       description as currency_name, from source)

select * from renamed

{% endif %}

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
