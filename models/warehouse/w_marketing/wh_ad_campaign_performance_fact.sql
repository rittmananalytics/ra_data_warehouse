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
),
 web_events as (
   SELECT * from {{ ref('wh_web_events_fact') }}
 ),
campaign_performance_joined as (
select GENERATE_UUID() as ad_campaign_performance_pk,
       s.ad_campaign_pk,
       s.utm_source,
       s.utm_campaign,
       c.* except (ad_campaign_id),
from campaign_performance c
join campaigns s
on c.ad_campaign_id = s.ad_campaign_id)
,
  segment_clicks AS (
  SELECT
    utm_source,
    utm_campaign,
    campaign_date,
    SUM(total_clicks) AS total_clicks
  FROM (
    SELECT
      utm_source,
      LOWER(utm_campaign) AS utm_campaign,
      utm_term,
      TIMESTAMP_TRUNC(event_ts,DAY) AS campaign_date,

      blended_user_id,

      COUNT(web_event_pk) AS total_clicks
    FROM
      web_events
    WHERE
      utm_source IN ('adwords',
        'facebook','instagra')
    GROUP BY
      1,2,3,4,5)
  GROUP BY
    1,2,3),
  ad_network_clicks AS (
  SELECT
    utm_source,
    utm_campaign,
    campaign_date,
    SUM(total_reported_cost) AS total_reported_cost,
    AVG(avg_reported_time_on_site) AS avg_reported_time_on_site,
    AVG(avg_reported_bounce_rate) AS avg_reported_bounce_rate,
    SUM(total_reported_clicks) AS total_reported_clicks,
    SUM(total_reported_impressions) AS total_reported_impressions,
    SUM(total_reported_invalid_clicks) AS total_reported_invalid_clicks
  FROM (
    SELECT
      TIMESTAMP_TRUNC(ad_campaign_serve_ts,DAY) AS campaign_date,
      utm_source,
      utm_campaign,
      ad_campaign_total_cost AS total_reported_cost,
      ad_campaign_avg_time_on_site AS avg_reported_time_on_site,
      ad_campaign_bounce_rate AS avg_reported_bounce_rate,
      ad_campaign_total_clicks AS total_reported_clicks,
      ad_campaign_total_impressions AS total_reported_impressions,
      ad_campaign_total_invalid_clicks AS total_reported_invalid_clicks
    FROM
      campaign_performance_joined)
  GROUP BY
    1,2,3),
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
LEFT JOIN
  segment_clicks s
ON
  s.utm_source = a.utm_source
  AND s.utm_campaign = a.utm_campaign
  AND s.campaign_date = a.campaign_date
)
select * from joined
where utm_source is not null
and utm_campaign is not null 
