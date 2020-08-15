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
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
  ),
{% if var("enable_subscriptions_warehouse")  %}
    customers as (
   SELECT *
    FROM   {{ ref('wh_customers_dim') }}
     ),
joined as (
SELECT
    c.customer_pk,
    s.* 
FROM
   sessions s
LEFT OUTER JOIN customers c
   ON s.blended_user_id = c.customer_id),
{% else %}    
joined as (
SELECT
   s.* 
FROM
   sessions s
),
{% endif %}
ordered as (
select GENERATE_UUID() as web_sessions_pk,
        * ,
        row_number() over (partition by blended_user_id order by session_start_ts) as user_session_number
from joined)
select *,
       lag(channel,1) over (partition by blended_user_id order by session_start_ts) as prev_session_channel,
       lag(utm_medium) over (partition by blended_user_id order by session_start_ts) as prev_utm_medium,
       lag(utm_source) over (partition by blended_user_id order by session_start_ts) as prev_utm_source,
from ordered
