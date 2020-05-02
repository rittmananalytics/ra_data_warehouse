{% if not var("enable_segment_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


{% set sessionization_cutoff %}
(
    select
        {{ dbt_utils.dateadd(
            'hour',
            -var('segment_sessionization_trailing_window'),
            'max(session_start_tstamp)'
        ) }}
    from {{this}}
)
{% endset %}

{#
Window functions are challenging to make incremental. This approach grabs
existing values from the existing table and then adds the value of session_number
on top of that seed. During development, this decreased the model runtime
by 25x on 2 years of data (from 600 to 25 seconds), so even though the code is
more complicated, the performance tradeoff is worth it.
#}

with sessions as (

    select * from {{ref('stg_segment_events_web_sessions_stitched')}}

    {% if is_incremental() %}
    where cast(session_start_tstamp as datetime) > {{sessionization_cutoff}}
    {% endif %}

),

{% if is_incremental() %}

agg as (

    select
        blended_user_id,
        count(*) as starting_session_number
    from {{this}}

    -- only include sessions that are not going to be resessionized in this run
    where cast(session_start_tstamp as datetime) <= {{sessionization_cutoff}}

    group by 1

),

{% endif %}

windowed as (

    select

        *,

        row_number() over (
            partition by blended_user_id
            order by sessions.session_start_tstamp
            )
            {% if is_incremental() %}+ coalesce(agg.starting_session_number, 0) {% endif %}
            as session_number

    from sessions

    {% if is_incremental() %}
    left join agg using (blended_user_id)
    {% endif %}


)

select * from windowed
