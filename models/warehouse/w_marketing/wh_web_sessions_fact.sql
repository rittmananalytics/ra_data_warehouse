{% if (not var("enable_segment_events_source") and var("enable_mixpanel_events_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='web_sessions_fact'
    )
}}
{% endif %}

with sessions as
  (
    select * from (SELECT
  session_id,
  session_start_ts,
  session_end_ts,
  events,
  utm_source,
  utm_content,
  utm_medium,
  utm_campaign,
  utm_term,
  search,
  gclid,
  first_page_url,
  first_page_url_host,
  first_page_url_path,
  referrer_host,
  device,
  device_category,
  last_page_url,
  last_page_url_host,
  last_page_url_path,
  duration_in_s,
  duration_in_s_tier,
  referrer_medium,
  referrer_source,
  channel,
  blended_user_id,
  sum(mins_between_sessions) over (partition by session_id) as mins_between_sessions,
  is_bounced_session
FROM
  {{ ref('int_web_events_sessions_stitched') }}
  )
{{ dbt_utils.group_by(n=28) }}
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
joined as (
SELECT
    c.customer_pk,
    s.*,
    a.ad_campaign_pk
FROM
   sessions s
LEFT OUTER JOIN customers c
   ON s.blended_user_id = c.customer_id
LEFT OUTER JOIN utm_campaign_mapping m
   ON s.utm_campaign = m.utm_campaign
LEFT OUTER JOIN ad_campaigns a
         ON m.ad_campaign_id = a.ad_campaign_id
 ),
{% else %}
joined as (
SELECT
   s.*,a.ad_campaign_pk
FROM
   sessions s
LEFT OUTER JOIN utm_campaign_mapping m
      ON s.utm_campaign = m.utm_campaign
      AND s.utm_source = m.utm_source
LEFT OUTER JOIN ad_campaigns a
      ON m.ad_campaign_id = a.ad_campaign_id
),
{% endif %}
ordered as (
select {{ dbt_utils.surrogate_key(['session_id']) }} as web_sessions_pk,
        * ,
        row_number() over (partition by blended_user_id order by session_start_ts) as user_session_number
from joined)
select * from ordered
