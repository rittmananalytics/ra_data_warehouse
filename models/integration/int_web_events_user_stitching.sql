{% if not var("enable_segment_events_source") and not var("enable_mixpanel_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with events as (

    select * from {{ ref('int_web_events') }}

),

mapping as (

    select distinct

        visitor_id,

        last_value(user_id ignore nulls) over (
            partition by visitor_id
            order by event_ts
            rows between unbounded preceding and unbounded following
        ) as user_id,

        min(event_ts) over (
            partition by visitor_id
        ) as first_seen_at,

        max(event_ts) over (
            partition by visitor_id
        ) as last_seen_at

    from events

)

select * from mapping
