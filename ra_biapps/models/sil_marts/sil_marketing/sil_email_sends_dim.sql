{{
    config(
        unique_key='send_pk',
        alias='email_sends_dim'
    )
}}
WITH sends AS
  (
  SELECT * from {{ ref('sde_email_sends_ds') }}
)
select GENERATE_UUID() as send_pk,
       s.*
from sends s
