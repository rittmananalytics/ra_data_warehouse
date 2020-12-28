{% if not var("enable_finance_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='transactions_fact',
        unique_key='transaction_id',
        incremental_strategy='merge',
        materialized='incremental'
    )
}}
{% endif %}

WITH transactions AS
  (
  SELECT *
  FROM   {{ ref('int_transactions') }}
  )

SELECT
   {{ dbt_utils.surrogate_key(['transaction_id']) }}  as transaction_pk,
   *
FROM
   transactions

   {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where transaction_created_ts > (select max(transaction_created_ts) from {{ this }})
        or    transaction_last_modified_ts > (select max(transaction_last_modified_ts) from {{ this }})
   {% endif %}
