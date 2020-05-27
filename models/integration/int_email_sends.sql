{% if not var("enable_mailchimp_email_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with t_email_campaigns_merge_list as
  (
    SELECT *
    FROM   {{ ref('stg_mailchimp_email_sends') }}
  )
select * from t_email_campaigns_merge_list
