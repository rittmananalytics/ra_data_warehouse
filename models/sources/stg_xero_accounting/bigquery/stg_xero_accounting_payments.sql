{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_payment_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_payment_sources") %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}


with source as (
  select *
  from {{ source('fivetran_xero_accounting','payment') }}
),
renamed as (
  SELECT
    concat('{{ var('stg_xero_accounting_id-prefix') }}',payment_id) as payment_id,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',account_id) as account_id,
    cast(null as {{ dbt_utils.type_string() }}) as payment_code,
    cast(null as {{ dbt_utils.type_string() }}) as company_id,
    cast(null as {{ dbt_utils.type_boolean() }}) as payment_is_discounted,
    cast(null as {{ dbt_utils.type_string() }}) as currency_code,
    cast(null as {{ dbt_utils.type_string() }}) as invoice_number,
    concat('{{ var('stg_xero_accounting_id-prefix') }}',invoice_id) as invoice_id,
    cast(null as {{ dbt_utils.type_string() }}) as invoice_type,
    status as payment_status,
    payment_type as payment_type,
    reference as payment_reference,
    amount as payment_amount,
    timestamp(date) as payment_date,
    is_reconciled as payment_is_reconciled,
    bank_amount as payment_bank_amount,
    currency_rate as payment_currency_rate
  FROM source
)
{% endif %}
select * from renamed



{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
