{% if not var("enable_mailchimp_email_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        unique_key='list_pk',
        alias='email_lists_dim'
    )
}}
{% endif %}

WITH lists AS
  (
  SELECT * from {{ ref('sde_email_lists_ds') }}
)
select GENERATE_UUID() as list_pk,
       l.*
from lists l
