{% if not var("enable_mailchimp_email") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with sde_email_campaign_events_merge_list as
  (
    SELECT *
    FROM   {{ ref('sde_mailchimp_email_events') }}
  )
select * from sde_email_campaign_events_merge_list
