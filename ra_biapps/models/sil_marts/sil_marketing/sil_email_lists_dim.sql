{{
    config(
        unique_key='list_pk',
        alias='email_lists_dim'
    )
}}
WITH lists AS
  (
  SELECT * from {{ ref('sde_email_lists_ds') }}
)
select GENERATE_UUID() as list_pk,
       l.*
from lists l
