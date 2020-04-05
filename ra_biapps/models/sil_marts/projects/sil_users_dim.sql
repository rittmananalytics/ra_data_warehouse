{{
    config(
        unique_key='user_pk',
        alias='user_dim'
    )
}}
WITH users AS
  (
  SELECT * from {{ ref('sde_users_ds') }}
)
select GENERATE_UUID() as user_pk,
       u.*
from users u
