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
  SELECT * FROM {{ ref('int_email_lists') }}
)
SELECT {{ dbt_utils.surrogate_key(['list_id']) }} AS list_pk,
       l.*
FROM lists l
