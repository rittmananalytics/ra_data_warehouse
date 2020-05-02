{% if not var("enable_segment_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}
with events as (

    select * from {{ source('segment_events', 's_pages') }}

),

mapping as (

    select distinct

        anonymous_id,

        last_value(user_id ignore nulls) over (
            partition by anonymous_id
            order by `timestamp`
            rows between unbounded preceding and unbounded following
        ) as user_id,

        min(`timestamp`) over (
            partition by anonymous_id
        ) as first_seen_at,

        max(`timestamp`) over (
            partition by anonymous_id
        ) as last_seen_at

    from events

)

select * from mapping
