{% if var("finance_warehouse_transaction_sources") %}

{{
    config(
        alias='transactions_fact',
        unique_key='transaction_id',
        incremental_strategy='merge',
        materialized='incremental'
    )
}}


WITH transactions AS
  (
  SELECT *
  FROM   {{ ref('int_transactions') }}
  )

SELECT
   {{ dbt_utils.surrogate_key(['transaction_id']) }}  AS transaction_pk,
   *
FROM
   transactions

   {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where transaction_created_ts > (SELECT max(transaction_created_ts) FROM {{ this }})
        or    transaction_last_modified_ts > (SELECT max(transaction_last_modified_ts) FROM {{ this }})
   {% endif %}

   {% else %} {{config(enabled=false)}} {% endif %}
