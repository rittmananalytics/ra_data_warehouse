{{
    config(
        unique_key='currency_pk',
        alias='currency_dim'
    )
}}
WITH currencies AS
  (
  SELECT *
  FROM   {{ ref('sde_currencies_ds') }}
  )

SELECT
   GENERATE_UUID() as account_pk,
   *
FROM
   currencies
