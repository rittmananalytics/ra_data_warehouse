{% if not var("enable_facebook_ads_source") and (not var("enable_marketing_warehouse") and (not var("enable_google_ads_source") ) ) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with ad_performance as
  (
    {% if var("enable_facebook_ads_source") %}
    SELECT *
    FROM   {{ ref('stg_facebook_ads_ad_performance') }}
    {% endif %}

    {% if var("enable_facebook_ads_source") and var("enable_google_ads_source") and not var("stg_google_ads_campaigns_only") %}
  %}
    UNION All
    {% endif %}

    {% if var("enable_google_ads_source")  %}
    SELECT *
    FROM   {{ ref('stg_google_ads_ad_performance') }}
    {% endif %}
  )
select * from ad_performance
