with sde_currencies_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_xero_accounting_currencies') }}
  )
select * from sde_currencies_merge_list
