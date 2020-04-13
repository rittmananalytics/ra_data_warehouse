{% if not var("enable_mailchimp_email") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with sde_email_campaigns_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_mailchimp_email_sends') }}
  )
select * from sde_email_campaigns_merge_list
