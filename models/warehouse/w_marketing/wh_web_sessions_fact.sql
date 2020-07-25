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
    SELECT *
    FROM   {{ ref('int_web_events_sessions_stitched') }}
  ),
unique_sessions as (
SELECT
    session_id,
    MAX(visitor_id) as visitor_id,
    MAX(user_id) as user_id,
    MAX(session_start_ts) as session_start_ts,
    MAX(session_end_ts) as session_end_ts,
    MAX(events) as events,
    MAX(channel) as channel,
    MAX(utm_source) as utm_source,
    MAX(utm_content) as utm_content,
    MAX(utm_medium) as utm_medium,
    MAX(utm_campaign) as utm_campaign,
    MAX(utm_term) as utm_term,
    MAX(search) as search,
    MAX(gclid) as gclid,
    MAX(first_page_url) as first_page_url,
    MAX(first_page_url_host) as first_page_url_host,
    MAX(first_page_url_path) as first_page_url_path,
    MAX(referrer_host) as referrer_host,
    MAX(device) as device,
    MAX(device_category) as device_category,
    MAX(last_page_url) as last_page_url,
    MAX(last_page_url_host) as last_page_url_host,
    MAX(last_page_url_path) as last_page_url_path,
    MAX(duration_in_s) as duration_in_s,
    MAX(duration_in_s_tier) as duration_in_s_tier,
    MAX(referrer_medium) as referrer_medium,
    MAX(referrer_source) as referrer_source,
    MAX(blended_user_id) as blended_user_id,
    MAX(mins_between_sessions) as mins_between_sessions,
    MAX(is_bounced_session) as is_bounced_session
from sessions
group by 1),
ordered as (
select GENERATE_UUID() as web_sessions_pk,
        * ,
        row_number() over (partition by blended_user_id order by session_start_ts) as user_session_number
from unique_sessions)
select *,
       lag(channel,1) over (partition by blended_user_id order by user_session_number) as prev_session_channel
from ordered
