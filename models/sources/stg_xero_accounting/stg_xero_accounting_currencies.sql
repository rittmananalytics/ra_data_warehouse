{% if not var("enable_xero_accounting_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
  {{ filter_stitch_table(var('stg_xero_accounting_stitch_schema'),var('stg_xero_accounting_stitch_currencies_table'),'code') }}
),
renamed as (
select code currency_code,
       description as currency_name, from source)
select * from renamed
