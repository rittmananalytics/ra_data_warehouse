with sde_chart_of_accounts_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_xero_accounting_accounts') }}
  )
SELECT * from sde_chart_of_accounts_merge_list
