{% if var('product_warehouse_event_sources') %}



with sessions AS (

    SELECT * FROM {{ref('int_web_events_sessions_initial')}}

    {% if is_incremental() %}
        where CAST(session_start_ts AS datetime) > (
          SELECT
            {{ dbt_utils.dateadd(
                'hour',
                -var('web_sessionization_trailing_window'),
                'max(session_start_ts)'
            ) }}
          FROM {{ this }})
    {% endif %}

),

id_stitching AS (

    SELECT * FROM {{ref('int_web_events_user_stitching')}}

),

joined AS (

    SELECT

        sessions.*,

        coalesce(id_stitching.user_id, sessions.visitor_id)
            AS blended_user_id

    FROM sessions
    left join id_stitching using (visitor_id)

)

SELECT *,
       {{ dbt_utils.datediff('lead(session_start_ts, 1) OVER (PARTITION BYblended_user_id ORDER BY session_start_ts DESC)','session_start_ts','MINUTE') }} AS mins_between_sessions,
       case when events = 1 then true else false end AS is_bounced_session



        FROM joined

{% else %}

  {{config(enabled=false)}}

{% endif %}
