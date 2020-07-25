{% if not var("enable_facebook_ads_source") and (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with campaigns as
  (
    {% if var("enable_facebook_ads_source") %}
    SELECT *
    FROM   {{ ref('stg_facebook_ads_ad_groups') }}
    {% endif %}

    {% if var("enable_facebook_ads_source") and var("enable_google_ads_source")  %}
    UNION All
    {% endif %}

    {% if var("enable_google_ads_source")  %}
    SELECT *
    FROM   {{ ref('stg_google_ads_ad_groups') }}
    {% endif %}
  )
select * from campaigns
