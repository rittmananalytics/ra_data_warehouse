{% if not var("enable_facebook_ads_source") and (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with ad_campaigns as
  (
    
    {% if var("enable_google_ads_source")  %}
    SELECT *
    FROM   {{ ref('stg_google_ads_campaign_performance') }}
    {% endif %}
  )
select * from ad_campaigns
