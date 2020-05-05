{% if not var("enable_xero_accounting_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with source as (
{{ filter_source('xero_accounting','s_currencies','code') }}
),
renamed as (
select code currency_code,
       description as currency_name, from source)
select * from renamed