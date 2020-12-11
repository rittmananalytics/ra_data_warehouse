{% if (not var("enable_facebook_ads_source") and not var("enable_marketing_warehouse"))  %}
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

    {% if var("enable_facebook_ads_source") and var("enable_google_ads_source")  %}
    UNION All
    {% endif %}

    {% if var("enable_google_ads_source")  %}
    SELECT *
    FROM   {{ ref('stg_google_ads_campaigns') }}
    {% endif %}

    {% if (var("enable_facebook_ads_source") or var("enable_google_ads_source")) and  var("enable_mailchimp_email_source") %}
    UNION All
    {% endif %}

    {% if var("enable_mailchimp_email_source")  %}
    SELECT *
    FROM   {{ ref('stg_mailchimp_email_campaigns') }}
    {% endif %}

    {% if (var("enable_facebook_ads_source") or var("enable_google_ads_source") or var("enable_mailchimp_email_source")) and  var("enable_hubspot_crm_source") %}
    UNION All
    {% endif %}

    {% if var("enable_hubspot_crm_source")  %}
    SELECT *
    FROM   {{ ref('stg_hubspot_crm_campaigns') }}
    {% endif %}
  )
select *,

       case when ad_network = 'Google Ads' then 'adwords'
            when ad_network = 'Facebook Ads' then 'facebook'
            when ad_network = 'Mailchimp' then 'newsletter'
            when ad_network = 'Hubspot Email' then 'hs_email'
            end as utm_source,
       case when ad_network in ('Google Ads','Facebook Ads') then 'paid'
            when ad_network in ('Mailchimp','Hubspot Email') then 'email'
            else null end as utm_medium)
       case when ad_campaign_name like '%Winter 2019%' then 'winter_2019'
            when ad_campaign_name like '%Summer 2020%' then 'summer_2020'
            when ad_campaign_name = 'Rittman Analytics Newsletter December 2020' then 'Analytics Solutions December 2020'
       else lower(ad_campaign_name) end as utm_campaign,
 from campaigns
