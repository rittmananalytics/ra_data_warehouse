{% if not var("enable_facebook_ads_source") or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with campaigns as
  (
    SELECT *
    FROM   {{ ref('stg_facebook_ads_campaigns') }}
  )
select * from campaigns
