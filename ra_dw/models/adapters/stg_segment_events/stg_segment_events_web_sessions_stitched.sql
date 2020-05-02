{% if not var("enable_segment_events_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

with sessions as (

    select * from {{ref('stg_segment_events_web_sessions_initial')}}

    {% if is_incremental() %}
        where cast(session_start_tstamp as datetime) > (
          select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(session_start_tstamp)'
            ) }}
          from {{ this }})
    {% endif %}

),

id_stitching as (

    select * from {{ref('stg_segment_events_web_user_stitching')}}

),

joined as (

    select

        sessions.*,

        coalesce(id_stitching.user_id, sessions.anonymous_id)
            as blended_user_id

    from sessions
    left join id_stitching using (anonymous_id)

)

select * from joined
