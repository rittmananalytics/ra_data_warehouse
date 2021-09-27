{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_currencies_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_currencies_sources") %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}

with source as (
  select *
  from {{ source('fivetran_xero_accounting','currency') }}

),
renamed as (
select concat('{{ var('stg_xero_accounting_id-prefix') }}',code) as currency_code,
       description as currency_name, from source)

select * from renamed

{% endif %}

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
