{% if var("finance_warehouse_bank_transaction_sources") %}

{{
    config(
        alias='bank_transactions_fact',
        unique_key='bank_transaction_pk',
        materialize='table'
    )
}}


WITH bank_transactions AS
  (
  SELECT *
  FROM   {{ ref('int_bank_transactions') }}
  )

SELECT
   {{ dbt_utils.surrogate_key(['bank_transaction_id']) }}  as bank_transaction_pk,
   *
FROM
   bank_transactions

   {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where transaction_created_ts > (select max(transaction_created_ts) from {{ this }})
        or    transaction_last_modified_ts > (select max(transaction_last_modified_ts) from {{ this }})
   {% endif %}

   {% else %} {{config(enabled=false)}} {% endif %}
