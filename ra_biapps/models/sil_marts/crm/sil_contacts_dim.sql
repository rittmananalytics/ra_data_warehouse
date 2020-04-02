{{
    config(
        unique_key='contact_pk',
        alias='contacts_dim'
    )
}}
WITH contacts AS
  (
  SELECT *
  FROM   {{ ref('sde_contacts_ds') }}
)
select    GENERATE_UUID() as contact_pk,
          * 
          FROM
          contacts c
