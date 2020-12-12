{% if (not var("enable_segment_events_source") and var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='web_events_fact'
    )
}}
{% endif %}

with events as
  (
    SELECT *
    FROM   {{ ref('int_web_events_sessionized') }}
  ),
utm_campaign_mapping as
( SELECT *
  FROM {{ ref('utm_campaign_mapping')}}
),
ad_campaigns as (
  SELECT *
    FROM {{ ref('wh_ad_campaigns_dim')}}
),
{% if var("enable_subscriptions_warehouse")  %}
    customers as (
   SELECT *
    FROM   {{ ref('wh_customers_dim') }}
  ),
{% endif %}
events_with_prev_ts_event_type as
(
SELECT

    GENERATE_UUID() as web_event_pk,
    e.*,

    lag(e.event_ts,1) over (partition by e.blended_user_id order by event_seq) as prev_event_ts,
    lag(e.event_type,1)  over (partition by e.blended_user_id order by event_seq) as prev_event_type
FROM
   events e
)
{% if var("enable_subscriptions_warehouse")  %}
,
joined as
(
  SELECT
      e.*,
      a.ad_campaign_pk,
      c.customer_pk
  FROM
     events_with_prev_ts_event_type e
  LEFT OUTER JOIN customers c
     ON e.user_id = c.customer_id
  LEFT OUTER JOIN utm_campaign_mapping m
     ON e.utm_campaign = m.utm_campaign
     AND e.utm_source = m.utm_source
  LEFT OUTER JOIN ad_campaigns a
           ON m.ad_campaign_id = a.ad_campaign_id
)
select * from joined
{% else %}
select e.*,
       a.ad_campaign_pk
from events_with_prev_ts_event_type e
LEFT OUTER JOIN utm_campaign_mapping m
   ON e.utm_campaign = m.utm_campaign
   AND e.utm_source = m.utm_source
LEFT OUTER JOIN ad_campaigns a
         ON m.ad_campaign_id = a.ad_campaign_id
{% endif %}
