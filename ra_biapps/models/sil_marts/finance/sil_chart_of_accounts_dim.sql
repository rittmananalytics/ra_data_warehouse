{{
    config(
        unique_key='account_pk',
        alias='chart_of_accounts_dim'
    )
}}
WITH chart_of_accounts AS
  (
  SELECT *
  FROM   {{ ref('sde_chart_of_accounts_ds') }}
  )

SELECT
   GENERATE_UUID() as account_pk,
   *
FROM
   chart_of_accounts
