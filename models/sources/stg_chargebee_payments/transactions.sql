{{ config(
  materialized = 'table'
) }}

with transactions as (
  select * from {{ ref( 'chargebee_transactions' )}}
)

select * from transactions
