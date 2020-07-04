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
select * from events_with_prev_ts_event_type
