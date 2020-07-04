{% if (not var("enable_segment_events_source") and var("enable_mixpanel_events_source") and var("enable_stripe_subscriptions_source")) or (not var("enable_marketing_warehouse")) %}
{{
    config(
        enabled=false
    )
}}
{% else %}
{{
    config(
        alias='web_event_subscr_plans_fact'
    )
}}
{% endif %}
WITH
  subscriptions AS (
  SELECT
    *
    FROM
      {{ ref('wh_subscriptions_fact') }}
    ),
  plans as (
    SELECT
      *
      FROM
        {{ ref('wh_plans_dim') }}
  ),
  page_classification as (
    select * from {{ ref('page_classification')}}
  )
SELECT s.*,
       su.subscription_status as subscription_status,
       coalesce(p.plan_name,'None') as plan_name,
       p.plan_interval,
       case when su.subscription_status = 'active' then s.customer_pk end as active_customer_pk,
       case when su.subscription_status = 'trialing' then s.customer_pk end as trialing_customer_pk,
       case when su.subscription_status in ('incomplete','incomplete_expired','past_due','cancelled') then s.customer_pk end as incomplete_expired_or_cancelled_customer_pk,

      array_agg(struct(e.web_event_pk,
      case when e.event_type = 'account_created' then s.web_sessions_pk end as new_trialling_session_pk,
      case when e.event_type = 'subscribed' then s.customer_pk end as new_converting_session_pk,
      e.event_id ,
      e.event_in_session_seq as session_event_num,
      e.event_seq as event_num,
      e.city_name as city,
      e.country_name as country,
      e.continent_name as continent,
      e.metro_code as metro,
      case when e.latitude is not null then concat(e.latitude,',',e.longitude) end as lat_long,
      e.event_type as event_type,
      case when e.event_type = 'Page View' then 1 end as page_view_count,
      e.event_ts as event_ts, e.event_details as event_details,
      e.page_url as page_url, coalesce(l.category,'Uncategorized') as category,
      e.time_on_page_secs as time_on_page_secs,
      e.page_url_host,
      e.page_title as page_title,
      e.page_url_path as page_url_path,
      e.converting_page_url,
      e.converting_page_title,
      e.pre_converting_page_url,
      e.pre_converting_page_title,
      e.prev_event_ts,
      e.prev_event_type,
      e.site)) event
FROM {{ ref('wh_web_sessions_fact') }} s
join {{ ref('wh_web_events_fact') }} e
on s.session_id = e.session_id
left outer join subscriptions su
on s.customer_pk = su.customer_pk
and e.event_ts between su.subscription_current_period_start_date and su.subscription_current_period_end_date
left outer join plans p
on su.plan_id = p.plan_id
left outer join page_classification l
on split(e.page_url,'?')[safe_offset(0)] = l.url
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38
