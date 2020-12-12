{% if not var("enable_marketing_warehouse") %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
      alias='ad_campaign_performance_fact'
    )
}}
{% endif %}
WITH
  campaign_performance AS
  (
  SELECT * from {{ ref('int_ad_campaign_performance') }}
),
  campaigns as (
  SELECT * from {{ ref('wh_ad_campaigns_dim') }}
  )
,
 web_events as (
   SELECT * from {{ ref('wh_web_events_fact') }}
 ),
campaign_performance_joined as (
  SELECT
       s.ad_campaign_pk,
       s.utm_source,
       s.utm_campaign,
       s.utm_medium,
       c.*
from campaign_performance c
left join campaigns s
on c.ad_campaign_id = s.ad_campaign_id)
,
  segment_clicks AS (
    SELECT
      ad_campaign_pk,
      TIMESTAMP_TRUNC(event_ts,DAY) AS campaign_date,
      COUNT(web_event_pk) AS total_clicks
    FROM
      web_events
    WHERE
      ad_campaign_pk is not null
    GROUP BY
      1,2),
  ad_network_clicks AS (
  SELECT
    ad_campaign_pk,
    campaign_date,
    utm_source,
    utm_campaign,
    utm_medium,
    SUM(total_reported_cost) AS total_reported_cost,
    AVG(avg_reported_time_on_site) AS avg_reported_time_on_site,
    AVG(avg_reported_bounce_rate) AS avg_reported_bounce_rate,
    SUM(total_reported_clicks) AS total_reported_clicks,
    SUM(total_reported_impressions) AS total_reported_impressions,
    SUM(total_reported_invalid_clicks) AS total_reported_invalid_clicks
  FROM (
    SELECT
      TIMESTAMP_TRUNC(ad_campaign_serve_ts,DAY) AS campaign_date,
      ad_campaign_pk,
      utm_source,
      utm_campaign,
      utm_medium,
      case when utm_source = 'newsletter' then ad_campaign_total_cost
           when utm_source = 'adwords' then (ad_campaign_total_cost*.75)
           else ad_campaign_total_cost end AS total_reported_cost,
      ad_campaign_avg_time_on_site AS avg_reported_time_on_site,
      ad_campaign_bounce_rate AS avg_reported_bounce_rate,
      ad_campaign_total_clicks AS total_reported_clicks,
      ad_campaign_total_impressions AS total_reported_impressions,
      ad_campaign_total_invalid_clicks AS total_reported_invalid_clicks
    FROM
      campaign_performance_joined)
  GROUP BY
    1,2,3,4,5),
 joined as (
SELECT
  a.*,
  coalesce(s.total_clicks,0) as total_clicks,
  safe_divide(s.total_clicks,
    A.total_reported_clicks) AS actual_vs_reported_clicks_pct,
  safe_divide(a.total_reported_cost,
    a.total_reported_clicks) AS reported_cpc,
  safe_divide(a.total_reported_cost,
    s.total_clicks) AS actual_cpc,
  safe_divide(a.total_reported_clicks,
    a.total_reported_impressions) as reported_ctr,
    safe_divide(s.total_clicks,
      a.total_reported_impressions) as actual_ctr,
    safe_divide((a.total_reported_cost*1000),
      a.total_reported_impressions) as reported_cpm
FROM
  ad_network_clicks a
LEFT OUTER JOIN
  segment_clicks s
ON
  s.ad_campaign_pk = a.ad_campaign_pk
  AND s.campaign_date = a.campaign_date
)
select * from joined
--where utm_source is not null
--and utm_campaign is not null
