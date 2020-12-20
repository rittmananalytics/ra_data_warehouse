{% if (not var("enable_facebook_ads_source") and not var("enable_google_ads_source"))   %}
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
    {% if var("enable_mailchimp_email_source") and var("enable_google_ads_source")  %}
    UNION ALL
    {% endif %}
    {% if var("enable_mailchimp_email_source")  %}
    SELECT *
    FROM   {{ ref('stg_mailchimp_email_ad_campaign_performance') }}
    {% endif %}
    {% if (var("enable_facebook_ads_source") or var("enable_google_ads_source") or var("enable_mailchimp_email_source")) and  var("enable_hubspot_crm_source") %}
    UNION All
    {% endif %}
    {% if var("enable_hubspot_crm_source")  %}
    SELECT *
    FROM   {{ ref('stg_hubspot_email_ad_campaign_performance') }}
    {% endif %}


  )
select * from ad_campaigns
