{% if (not var("enable_facebook_ads_source") and not var("enable_google_ads_source")) or not var("ad_campaigns_only") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with campaigns as
  (
    {% if var("enable_facebook_ads_source") %}
    SELECT {{ dbt_utils.star(from=ref('stg_facebook_ads_ad_groups')) }}
    FROM   {{ ref('stg_facebook_ads_ad_groups') }}
    {% endif %}

    {% if var("enable_facebook_ads_source") and var("enable_google_ads_source") and var("stg_google_ads_enable_google_ads_ad_groups")  %}
    UNION All
    {% endif %}

    {% if var("enable_google_ads_source") and var("stg_google_ads_enable_google_ads_ad_groups") %}
    SELECT {{ dbt_utils.star(from=ref('stg_google_ads_ad_groups')) }}
    FROM   {{ ref('stg_google_ads_ad_groups') }}
    {% endif %}
  )
select * from campaigns
