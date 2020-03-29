{{
    config(
        unique_key='company_pk',
        alias='companies_dim'
    )
}}
WITH companies_dim as (
  SELECT
    GENERATE_UUID() as company_pk,
    *
  FROM
    {{ ref('sde_companies_ds') }} c
)
select * from companies_dim 
