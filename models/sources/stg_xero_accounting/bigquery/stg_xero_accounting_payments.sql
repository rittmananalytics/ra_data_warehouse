{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_payment_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_payment_sources") %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}


with source AS (
  SELECT *
  FROM {{ source('fivetran_xero_accounting','payment') }}
),
renamed AS (
  SELECT
    CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',payment_id) AS payment_id,
    CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',account_id) AS account_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS payment_code,
    CAST(null AS {{ dbt_utils.type_string() }}) AS company_id,
    CAST(null AS {{ dbt_utils.type_boolean() }}) AS payment_is_discounted,
    CAST(null AS {{ dbt_utils.type_string() }}) AS currency_code,
    CAST(null AS {{ dbt_utils.type_string() }}) AS invoice_number,
    CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',invoice_id) AS invoice_id,
    CAST(null AS {{ dbt_utils.type_string() }}) AS invoice_type,
    status AS payment_status,
    payment_type AS payment_type,
    reference AS payment_reference,
    amount AS payment_amount,
    timestamp(date) AS payment_date,
    is_reconciled AS payment_is_reconciled,
    bank_amount AS payment_bank_amount,
    currency_rate AS payment_currency_rate
  FROM source
)
{% endif %}
SELECT * FROM renamed



{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
