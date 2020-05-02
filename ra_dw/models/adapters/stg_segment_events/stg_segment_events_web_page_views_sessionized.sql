{% if not var("enable_segment_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{#
the initial CTE in this model is unusually complicated; its function is to
select all pageviews (for all time) for users who have pageviews since the
model was most recently run. there are many window functions in this model so
in order to appropriately calculate all of them we need each user's entire
page view history, but we only want to grab that for users who have page view
events we need to calculate.
#}

with pageviews as (

    select * from {{ref('stg_segment_events_web_page_views')}}

    {% if is_incremental() %}
    where anonymous_id in (
        select distinct anonymous_id
        from {{ref('stg_segment_events_web_page_views')}}
        where cast(tstamp as datetime) >= (
          select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(tstamp)'
            ) }}
          from {{ this }})
        )
    {% endif %}

),

numbered as (

    --This CTE is responsible for assigning an all-time page view number for a
    --given anonymous_id. We don't need to do this across devices because the
    --whole point of this field is for sessionization, and sessions can't span
    --multiple devices.

    select

        *,

        row_number() over (
            partition by anonymous_id
            order by tstamp
            ) as page_view_number

    from pageviews

),

lagged as (

    --This CTE is responsible for simply grabbing the last value of `tstamp`.
    --We'll use this downstream to do timestamp math--it's how we determine the
    --period of inactivity.

    select

        *,

        lag(tstamp) over (
            partition by anonymous_id
            order by page_view_number
            ) as previous_tstamp

    from numbered

),

diffed as (

    --This CTE simply calculates `period_of_inactivity`.

    select
        *,
        {{ dbt_utils.datediff('previous_tstamp', 'tstamp', 'second') }} as period_of_inactivity
    from lagged

),

new_sessions as (

    --This CTE calculates a single 1/0 field--if the period of inactivity prior
    --to this page view was greater than 30 minutes, the value is 1, otherwise
    --it's 0. We'll use this to calculate the user's session #.

    select
        *,
        case
            when period_of_inactivity <= {{var('segment_inactivity_cutoff')}} then 0
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
            partition by anonymous_id
            order by page_view_number
            rows between unbounded preceding and current row
            ) as session_number

    from new_sessions

),

session_ids as (

    --This CTE assigns a globally unique session id based on the combination of
    --`anonymous_id` and `session_number`.

    select

        {{dbt_utils.star(ref('stg_segment_events_web_page_views'))}},
        page_view_number,
        {{dbt_utils.surrogate_key('anonymous_id', 'session_number')}} as session_id

    from session_numbers

)

select * from session_ids
