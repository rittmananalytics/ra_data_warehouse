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
    FROM   {{ ref('stg_facebook_ads_campaigns') }}
    {% endif %}

    {% if var("enable_facebook_ads_source") and var("enable_google_ads_source") and not var("stg_google_ads_campaigns_only") %}
    UNION All
    {% endif %}

    {% if var("enable_google_ads_source")  %}
    SELECT *
    FROM   {{ ref('stg_google_ads_campaigns') }}
    {% endif %}
  )
select *,

       case when ad_network = 'Google Ads' then 'adwords'
            when ad_network = 'Facebook Ads' then 'facebook'
            end as utm_source,
       'paid' as utm_medium,
       lower(ad_campaign_name) as utm_campaign,
 from campaigns
