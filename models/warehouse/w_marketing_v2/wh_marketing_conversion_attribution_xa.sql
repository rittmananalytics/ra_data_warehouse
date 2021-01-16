{{ config(
    tags=["marketing"]
) }}

{{ config(
    materialized='table',
    alias='marketing_conversion_attribution_xa',
    unique_key='marketing_conversion_attribution_pk'
)}}

with conversion_events as
(

  select

    marketing_user_event_pk,
    platform_user_fk,
    marketing_user_session_fk,

    event_name as conversion_event_name,
    event_ts as conversion_event_ts

  from {{ ref('wh_marketing_user_events_fact') }}
  where event_name in ('install')

),

sessions as (

  select * from {{ ref('wh_marketing_user_sessions_fact')}}

),

pre_conversion_sessions as (

  select
    *

  from sessions

  left join conversion_events using (platform_user_fk)

  where sessions.starting_ts <= conversion_events.conversion_event_ts
  	and sessions.ending_ts >= timestamp_add(conversion_events.conversion_event_ts, interval -30 day)

),

index_sessions as (

  select
    *,

    count(*) over (
        partition by platform_user_fk
    ) as allocation_window_total_sessions,

    row_number() over (
        partition by platform_user_fk
        order by pre_conversion_sessions.starting_ts
    ) as allocation_window_session_number

from pre_conversion_sessions

),

allocate_points as (

  select
    *,
    case
        when allocation_window_total_sessions = 1 then 1.0
        when allocation_window_total_sessions = 2 then 0.5
        when allocation_window_session_number = 1 then 0.4
        when allocation_window_session_number = allocation_window_total_sessions then 0.4
        else 0.2 / (allocation_window_total_sessions - 2)
    end as forty_twenty_forty_points,

    case
        when allocation_window_session_number = 1 then 1.0
        else 0.0
    end as first_touch_points,

    case
        when allocation_window_session_number = allocation_window_total_sessions then 1.0
        else 0.0
    end as last_touch_points,

    1.0 / allocation_window_total_sessions as linear_points

  from index_sessions

),

final as (

  select
    {{ dbt_utils.surrogate_key(
      ['platform_user_fk', 'marketing_user_event_pk', 'marketing_user_session_pk']
    ) }} as marketing_conversion_attribution_pk,
    platform_user_fk,
    marketing_user_event_pk as marketing_user_event_fk,
    marketing_user_session_pk as marketing_user_session_fk,

    starting_ts as user_session_starting_ts,
    conversion_event_ts,

    conversion_event_name,
    initial_referrer,
    initial_referring_domain,
    referrer,
    referring_domain,
    search_engine,
    appsflyer_media_source,
    appsflyer_campaign,
    appsflyer_media_channel,

    first_touch_points,
    last_touch_points,
    forty_twenty_forty_points,
    linear_points

  from allocate_points

)

select * from final
order by platform_user_fk, user_session_starting_ts
