{% if not var("enable_mailchimp_email_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with source as (
  SELECT
    ad_campaign_serve_ts,
    ad_campaign_id,
    ad_campaign_budget,
    ad_campaign_avg_cost,
    ad_campaign_avg_time_on_site,
    ad_campaign_bounce_rate,
    ad_campaign_status,
    ad_campaign_total_assisted_conversions,
    sum(ad_campaign_total_clicks) as ad_campaign_total_clicks,
    ad_campaign_total_conversion_value,
    ad_campaign_total_conversions,
    sum(ad_campaign_total_cost) as ad_campaign_total_cost,
    sum(ad_campaign_total_engagements) as ad_campaign_total_engagements,
    sum(ad_campaign_total_impressions) as ad_campaign_total_impressions,
    sum(ad_campaign_total_invalid_clicks) as ad_campaign_total_invalid_clicks,
    ad_network
   FROM
  (
  SELECT
    TIMESTAMP(DATE(event_ts)) AS ad_campaign_serve_ts,
    send_id AS ad_campaign_id,
    NULL AS ad_campaign_budget,
    NULL AS ad_campaign_avg_cost,
    NULL AS ad_campaign_avg_time_on_site,
    NULL AS ad_campaign_bounce_rate,
    CAST(NULL AS string) AS ad_campaign_status,
    NULL AS ad_campaign_total_assisted_conversions,
    case when action = 'click' then 1 else 0 end AS ad_campaign_total_clicks,
    NULL AS ad_campaign_total_conversion_value,
    NULL AS ad_campaign_total_conversions,
    case when action = 'send' then 1*0.01642 else 0 end as ad_campaign_total_cost,
    case when action in ('open','click') then 1 else 0 end as ad_campaign_total_engagements,
    case when action = 'open' then 1 else 0 end as ad_campaign_total_impressions,
    case when action = 'bounce' then 1 else 0 end as ad_campaign_total_invalid_clicks,
   'Mailchimp' AS ad_network
    FROM
      {{ ref('stg_mailchimp_email_events') }})
    GROUP BY
      1,2,3,4,5,6,7,8,10,11,16)
select
  *
from
  source
