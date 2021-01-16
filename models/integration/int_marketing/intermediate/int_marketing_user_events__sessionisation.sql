{{ config(
    tags=["marketing"]
) }}

{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_session_sk',
    sort='event_ts',
    dist='user_session_sk'
  )
}}

with events as (

  select * from {{ ref('int_marketing_user_events__merge_sources') }}

  {% if is_incremental() %}
    where event_ts > (select max(event_ts) from {{ this }})
  {% endif %}

),

user_sessions as (

  select
    concat(cast(row_number() over(partition by platform_users_natural_key order by event_ts) as string), ' - ', platform_users_natural_key) as user_session_sk,
    platform_users_natural_key as session_user_id,
    event_ts as session_start_at,
    row_number() over(partition by platform_users_natural_key order by event_ts) as session_sequence_number,
    lead(event_ts) over(partition by platform_users_natural_key order by event_ts) as next_session_start_at

  from (
    select
      *,
      timestamp_diff(event_ts, lag(event_ts) over(partition by platform_users_natural_key order by event_ts), minute) as idle_time_minutes

    from events
  )

  where (idle_time_minutes > 30 or idle_time_minutes is null)

),

joined as (

  select
    e.*,
    s.*

  from events e
  inner join user_sessions s
    on e.platform_users_natural_key = s.session_user_id
    and e.event_ts >= s.session_start_at
    and (e.event_ts < s.next_session_start_at or s.next_session_start_at is null)

)

select * from joined
