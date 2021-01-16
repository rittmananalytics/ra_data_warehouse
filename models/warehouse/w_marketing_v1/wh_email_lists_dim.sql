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
  SELECT * from {{ ref('int_email_lists') }}
)
select {{ dbt_utils.surrogate_key(['list_id']) }} as list_pk,
       l.*
from lists l
