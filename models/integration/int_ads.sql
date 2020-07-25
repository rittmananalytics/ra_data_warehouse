{% if not var("enable_facebook_ads_source") and (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with ads as
  (
    {% if var("enable_facebook_ads_source") %}
    SELECT *
    FROM   {{ ref('stg_facebook_ads') }}
    {% endif %}

    {% if var("enable_facebook_ads_source") and var("enable_google_ads_source")  %}
    UNION All
    {% endif %}

    {% if var("enable_google_ads_source")  %}
    SELECT *
    FROM   {{ ref('stg_google_ads') }}
    {% endif %}
  ),
 ad_groups as (
   select * from {{ ref('int_ad_ad_groups') }}
 ),
 ad_campaigns as (
   select * from {{ ref('int_ad_campaigns') }}
 )
select
    a.ad_id,
    a.ad_status,
    a.ad_type,
    a.ad_final_urls,
    a.ad_group_id,
    a.ad_bid_type,
    a.ad_utm_parameters,
    lower(coalesce(a.ad_utm_campaign,c.ad_campaign_name)) as ad_utm_campaign,
    lower(a.ad_utm_content) as ad_utm_content,
    coalesce(a.ad_utm_medium,'paid') as ad_utm_medium,
    case when a.ad_network = 'Google Ads' then coalesce(a.ad_utm_source,'adwords')
         when a.ad_network = 'Facebook Ads' then coalesce(a.ad_utm_source,'facebook')
         end as ad_utm_source,
    a.ad_network
from ads a
left outer join ad_groups g
on a.ad_group_id = g.ad_group_id
left outer join ad_campaigns c
on g.ad_campaign_id = c.ad_campaign_id
