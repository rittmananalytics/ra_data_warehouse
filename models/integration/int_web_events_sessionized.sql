{% if not var("enable_segment_events_source") and not var("enable_mixpanel_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{#
the initial CTE in this model is unusually complicated; its function is to
select all events (for all time) for users who have pageviews since the
model was most recently run. there are many window functions in this model so
in order to appropriately calculate all of them we need each user's entire
event history, but we only want to grab that for users who have events we need to calculate.
#}

with events as (

    select * from {{ref('int_web_events')}}

    {% if is_incremental() %}
    where visitor_id in (
        select distinct visitor_id
        from {{ref('int_web_events')}}
        where cast(event_ts as datetime) >= (
          select
            {{ dbt_utils.dateadd(
                'hour',
                -var('web_sessionization_trailing_window'),
                'max(event_ts)'
            ) }}
          from {{ this }})
        )
    {% endif %}

),

numbered as (

    --This CTE is responsible for assigning an all-time event number for a
    --given visitor_id. We don't need to do this across devices because the
    --whole point of this field is for sessionization, and sessions can't span
    --multiple devices.

    select

        *,

        row_number() over (
            partition by visitor_id
            order by event_ts
          ) as event_number

    from events

),

lagged as (

    --This CTE is responsible for simply grabbing the last value of `event_ts`.
    --We'll use this downstream to do timestamp math--it's how we determine the
    --period of inactivity.

    select

        *,

        lag(event_ts) over (
            partition by visitor_id
            order by event_number
          ) as previous_event_ts

    from numbered

),

diffed as (

    --This CTE simply calculates `period_of_inactivity`.

    select
        *,
        {{ dbt_utils.datediff('previous_event_ts', 'event_ts', 'second') }} as period_of_inactivity
    from lagged

),

new_sessions as (

    --This CTE calculates a single 1/0 field--if the period of inactivity prior
    --to this page view was greater than 30 minutes, the value is 1, otherwise
    --it's 0. We'll use this to calculate the user's session #.

    select
        *,
        case
            when period_of_inactivity <= {{var('web_inactivity_cutoff')}} then 0
            else 1
        end as new_session
    from diffed

),

session_numbers as (

    --This CTE calculates a user's session (1, 2, 3) number from `new_session`.
    --This single field is the entire point of the entire prior series of
    --calculations.

    select

        *,

        sum(new_session) over (
            partition by visitor_id
            order by event_number
            rows between unbounded preceding and current row
            ) as session_number

    from new_sessions

),

session_ids as (

    --This CTE assigns a globally unique session id based on the combination of
    --`anonymous_id` and `session_number`.

    select

        {{dbt_utils.star(ref('int_web_events'))}},
        event_number,
        {{dbt_utils.surrogate_key('visitor_id', 'session_number')}} as session_id

    from session_numbers

),
id_stitching as (

    select * from {{ref('int_web_events_user_stitching')}}

),

joined as (

    select

        session_ids.*,

        coalesce(id_stitching.user_id, session_ids.visitor_id)
            as blended_user_id

    from session_ids
    left join id_stitching using (visitor_id)

)

select * from joined
