{% if var('product_warehouse_event_sources') %}



with sessions as (

    select * from {{ref('int_web_events_sessions_initial')}}

    {% if is_incremental() %}
        where cast(session_start_ts as datetime) > (
          select
            {{ dbt_utils.dateadd(
                'hour',
                -var('web_sessionization_trailing_window'),
                'max(session_start_ts)'
            ) }}
          from {{ this }})
    {% endif %}

),

id_stitching as (

    select * from {{ref('int_web_events_user_stitching')}}

),

joined as (

    select

        sessions.*,

        coalesce(id_stitching.user_id, sessions.visitor_id)
            as blended_user_id

    from sessions
    left join id_stitching using (visitor_id)

)

select *,
       {{ dbt_utils.datediff('lead(session_start_ts, 1) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts DESC)','session_start_ts','MINUTE') }} AS mins_between_sessions,
       case when events = 1 then true else false end as is_bounced_session



        from joined

{% else %}

  {{config(enabled=false)}}

{% endif %}
