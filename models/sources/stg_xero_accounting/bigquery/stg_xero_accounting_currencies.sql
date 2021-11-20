{{config(enabled = target.type == 'bigquery')}}
{% if var("finance_warehouse_currencies_sources") %}
{% if 'xero_accounting' in var("finance_warehouse_currencies_sources") %}

{% if var("stg_xero_accounting_etl") == 'fivetran' %}

with source AS (
  SELECT *
  FROM {{ source('fivetran_xero_accounting','currency') }}

),
renamed AS (
SELECT CONCAT('{{ var('stg_xero_accounting_id-prefix') }}',code) AS currency_code,
       description AS currency_name, FROM source)

SELECT * FROM renamed

{% endif %}

{% else %} {{config(enabled=false)}} {% endif %}
{% else %} {{config(enabled=false)}} {% endif %}
